//! Interface used by the engine to communicate with the plugin.

use std::{
    io::{BufRead, Write},
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc, Mutex, MutexGuard},
};

use nu_protocol::{PipelineData, ShellError, Value};

use crate::{
    plugin::{context::PluginExecutionContext, PluginEncoder},
    protocol::{
        EngineCall, EngineCallId, EngineCallResponse, ExternalStreamInfo, ListStreamInfo,
        PipelineDataHeader, PluginCall, PluginCallResponse, PluginCustomValue, PluginData,
        PluginInput, PluginOutput, RawStreamInfo, StreamId,
    },
};

use super::{
    buffers::StreamBuffers,
    make_pipe_external_stream, make_pipe_list_stream, next_id_from,
    stream_data_io::{
        StreamDataIo, StreamDataIoBase, StreamDataIoExt, StreamDataRead, StreamDataWrite,
    },
    PluginRead, PluginWrite,
};

#[cfg(test)]
mod tests;

pub(crate) struct PluginInterfaceImpl<R, W> {
    // Always lock read and then write mutex, if using both
    // Stream inputs that can't be handled immediately can be put on the buffer
    read: Mutex<ReadPart<R, W>>,
    write: Mutex<WritePart<W>>,
    context: Option<Arc<dyn PluginExecutionContext>>,
    /// The next available stream id
    next_stream_id: AtomicUsize,
}

pub(crate) struct ReadPart<R, W> {
    reader: R,
    /// Stores stream messages that can't be handled immediately
    stream_buffers: StreamBuffers,
    /// Keep the write type around
    write_marker: PhantomData<W>,
}

pub(crate) struct WritePart<W>(W);

impl<R, W> PluginInterfaceImpl<R, W> {
    pub(crate) fn new(
        reader: R,
        writer: W,
        context: Option<Arc<dyn PluginExecutionContext>>,
    ) -> PluginInterfaceImpl<R, W> {
        PluginInterfaceImpl {
            read: Mutex::new(ReadPart {
                reader,
                stream_buffers: StreamBuffers::default(),
                write_marker: PhantomData,
            }),
            write: Mutex::new(WritePart(writer)),
            context,
            next_stream_id: AtomicUsize::new(0),
        }
    }
}

impl<R, W> StreamDataIoBase for PluginInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    type ReadPart = ReadPart<R, W>;
    type WritePart = WritePart<W>;

    fn lock_read(&self) -> Result<MutexGuard<ReadPart<R, W>>, ShellError> {
        self.read.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in PluginInterface: read mutex poisoned due to panic".into(),
        })
    }

    fn lock_write(&self) -> Result<MutexGuard<WritePart<W>>, ShellError> {
        self.write.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in PluginInterface: write mutex poisoned due to panic".into(),
        })
    }

    fn new_stream_id(&self) -> Result<StreamId, ShellError> {
        next_id_from(&self.next_stream_id)
    }
}

impl<R, W> StreamDataRead for ReadPart<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    type Message = PluginOutput;
    type Base = PluginInterfaceImpl<R, W>;

    fn read(&mut self) -> Result<Option<Self::Message>, ShellError> {
        self.reader.read_output()
    }

    fn stream_buffers(&mut self) -> &mut StreamBuffers {
        &mut self.stream_buffers
    }

    // Default message handler
    fn handle_message(
        &mut self,
        io: &Arc<Self::Base>,
        msg: PluginOutput,
    ) -> Result<(), ShellError> {
        match msg {
            PluginOutput::CallResponse(_) => Err(ShellError::PluginFailedToDecode {
                msg: "unexpected CallResponse in this context".into(),
            }),
            // Handle out of order stream messages
            PluginOutput::Stream(stream_msg) => {
                self.handle_out_of_order(stream_msg)
            }
            // Execute an engine call during plugin execution
            PluginOutput::EngineCall(id, engine_call) => {
                io.clone().handle_engine_call(id, engine_call)
            }
        }
    }
}

impl<W> StreamDataWrite for WritePart<W>
where
    W: PluginWrite,
{
    type Message = PluginInput;

    fn write(&mut self, msg: Self::Message) -> Result<(), ShellError> {
        self.0.write_input(&msg)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        self.0.flush()
    }
}

/// The trait indirection is so that we can hide the types with a trait object inside
/// PluginInterface. As such, this trait must remain object safe.
pub(crate) trait PluginInterfaceIo: StreamDataIo {
    fn context(&self) -> Option<&Arc<dyn PluginExecutionContext>>;
    fn write_call(&self, call: PluginCall) -> Result<(), ShellError>;
    fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError>;

    /// Write an [EngineCallResponse] to the engine call with the given `id`.
    fn write_engine_call_response(
        &self,
        id: EngineCallId,
        response: EngineCallResponse,
    ) -> Result<(), ShellError>;

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError>;

    /// Create a valid header to send the given [`PipelineData`].
    ///
    /// Returns the header, and the arguments to be sent with `write_pipeline_data_stream`
    /// if necessary.
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<
        (
            PipelineDataHeader,
            Option<(PipelineDataHeader, PipelineData)>,
        ),
        ShellError,
    >;

    /// Handle an [`EngineCall`] during execution.
    fn handle_engine_call(
        self: Arc<Self>,
        id: EngineCallId,
        engine_call: EngineCall,
    ) -> Result<(), ShellError>;
}

impl<R, W> PluginInterfaceIo for PluginInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn context(&self) -> Option<&Arc<dyn PluginExecutionContext>> {
        self.context.as_ref()
    }

    fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
        let mut write = self.lock_write()?;
        log::trace!("Writing plugin call: {call:?}");

        write.write(PluginInput::Call(call))?;
        write.flush()?;

        log::trace!("Wrote plugin call");
        Ok(())
    }

    fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError> {
        log::trace!("Reading plugin call response");

        loop {
            let mut read = self.lock_read()?;
            match read.read()? {
                Some(PluginOutput::CallResponse(response)) => {
                    // Check the call input type to set the stream buffers up
                    if let PluginCallResponse::PipelineData(header) = &response {
                        read.stream_buffers.init_stream(header)?;
                    }
                    return Ok(response);
                }
                // Handle some other message
                Some(other) => read.handle_message(&self, other)?,
                // End of input
                None => {
                    return Err(ShellError::PluginFailedToDecode {
                        msg: "unexpected end of stream before receiving call response".into(),
                    })
                }
            }
        }
    }

    fn write_engine_call_response(
        &self,
        id: EngineCallId,
        response: EngineCallResponse,
    ) -> Result<(), ShellError> {
        let mut write = self.lock_write()?;
        log::trace!("Writing engine call response for id={id}: {response:?}");

        write.write(PluginInput::EngineCallResponse(id, response))?;
        write.flush()?;

        log::trace!("Wrote engine call response for id={id}");
        Ok(())
    }

    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        let context = self.context().ok_or_else(|| ShellError::NushellFailed {
            msg: "PluginExecutionContext must be provided to call make_pipeline_data".into(),
        })?;

        match header {
            PipelineDataHeader::Empty => Ok(PipelineData::Empty),
            PipelineDataHeader::Value(value) => Ok(PipelineData::Value(value, None)),
            PipelineDataHeader::PluginData(plugin_data) => {
                // Convert to PluginCustomData
                let value = Value::custom_value(
                    Box::new(PluginCustomValue {
                        name: plugin_data
                            .name
                            .ok_or_else(|| ShellError::PluginFailedToDecode {
                                msg: "String representation of PluginData not provided".into(),
                            })?,
                        data: plugin_data.data,
                        filename: context.filename().to_owned(),
                        shell: context.shell().map(|p| p.to_owned()),
                        source: context.command_name().to_owned(),
                    }),
                    plugin_data.span,
                );
                Ok(PipelineData::Value(value, None))
            }
            PipelineDataHeader::ListStream(info) => {
                let ctrlc = context.ctrlc().cloned();
                Ok(make_pipe_list_stream(self, &info, ctrlc))
            }
            PipelineDataHeader::ExternalStream(info) => {
                let ctrlc = context.ctrlc().cloned();
                Ok(make_pipe_external_stream(self, &info, ctrlc))
            }
        }
    }

    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<
        (
            PipelineDataHeader,
            Option<(PipelineDataHeader, PipelineData)>,
        ),
        ShellError,
    > {
        let context = self.context().ok_or_else(|| ShellError::NushellFailed {
            msg: "PluginExecutionContext must be provided to call make_pipeline_data_header".into(),
        })?;

        match data {
            PipelineData::Value(ref value @ Value::CustomValue { ref val, .. }, _) => {
                match val.as_any().downcast_ref::<PluginCustomValue>() {
                    Some(plugin_data) if plugin_data.filename == context.filename() => {
                        Ok((
                            PipelineDataHeader::PluginData(PluginData {
                                name: None, // plugin doesn't need it.
                                data: plugin_data.data.clone(),
                                span: value.span(),
                            }),
                            None,
                        ))
                    }
                    _ => {
                        let custom_value_name = val.value_string();
                        Err(ShellError::GenericError {
                            error: format!(
                                "Plugin {} can not handle the custom value {}",
                                context.command_name(),
                                custom_value_name
                            ),
                            msg: format!("custom value {custom_value_name}"),
                            span: Some(value.span()),
                            help: None,
                            inner: vec![],
                        })
                    }
                }
            }
            PipelineData::Value(Value::LazyRecord { ref val, .. }, _) => {
                Ok((PipelineDataHeader::Value(val.collect()?), None))
            }
            PipelineData::Value(value, _) => Ok((PipelineDataHeader::Value(value), None)),
            PipelineData::ListStream(_, _) => {
                let header = PipelineDataHeader::ListStream(ListStreamInfo {
                    id: self.new_stream_id()?,
                });
                Ok((header.clone(), Some((header, data))))
            }
            PipelineData::ExternalStream {
                span,
                ref stdout,
                ref stderr,
                ref exit_code,
                trim_end_newline,
                ..
            } => {
                let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
                    span,
                    stdout: if let Some(ref stdout) = stdout {
                        Some(RawStreamInfo::new(self.new_stream_id()?, stdout))
                    } else {
                        None
                    },
                    stderr: if let Some(ref stderr) = stderr {
                        Some(RawStreamInfo::new(self.new_stream_id()?, stderr))
                    } else {
                        None
                    },
                    exit_code: if exit_code.is_some() {
                        Some(ListStreamInfo {
                            id: self.new_stream_id()?,
                        })
                    } else {
                        None
                    },
                    trim_end_newline,
                });
                Ok((header.clone(), Some((header, data))))
            }
            PipelineData::Empty => Ok((PipelineDataHeader::Empty, None)),
        }
    }

    fn handle_engine_call(
        self: Arc<Self>,
        id: EngineCallId,
        engine_call: EngineCall,
    ) -> Result<(), ShellError> {
        let context = self.context().ok_or_else(|| ShellError::NushellFailed {
            msg: "PluginExecutionContext was not provided before making an engine call".into(),
        })?;

        log::trace!("Handling engine call id={id}: {engine_call:?}");

        match engine_call {
            EngineCall::GetConfig => {
                let config = context.get_config().clone().into();
                let response = EngineCallResponse::Config(config);
                self.write_engine_call_response(id, response)
            }
            EngineCall::EvalClosure {
                closure,
                positional,
                input,
                redirect_stdout,
                redirect_stderr,
            } => {
                // Build the input PipelineData
                let input = self.clone().make_pipeline_data(input)?;
                // Evaluate the closure
                match context.eval_closure(
                    closure,
                    positional,
                    input,
                    redirect_stdout,
                    redirect_stderr,
                ) {
                    Ok(output) => {
                        let (header, rest) = self.make_pipeline_data_header(output)?;
                        self.write_engine_call_response(
                            id,
                            EngineCallResponse::PipelineData(header),
                        )?;
                        // Write the stream if necessary
                        if let Some((header, data)) = rest {
                            self.write_pipeline_data_stream(&header, data)
                        } else {
                            Ok(())
                        }
                    }
                    Err(err) => self.write_engine_call_response(id, EngineCallResponse::Error(err)),
                }
            }
        }
    }
}

/// Implements communication and stream handling for a plugin instance.
#[derive(Clone)]
pub(crate) struct PluginInterface {
    io: Arc<dyn PluginInterfaceIo>,
}

impl<R, W> From<PluginInterfaceImpl<R, W>> for PluginInterface
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn from(plugin_impl: PluginInterfaceImpl<R, W>) -> Self {
        Arc::new(plugin_impl).into()
    }
}

impl<T> From<Arc<T>> for PluginInterface
where
    T: PluginInterfaceIo + 'static,
{
    fn from(value: Arc<T>) -> Self {
        PluginInterface { io: value }
    }
}

impl PluginInterface {
    /// Create the plugin interface from the given reader, writer, encoder, and context.
    pub(crate) fn new<R, W, E>(
        reader: R,
        writer: W,
        encoder: E,
        context: Option<Arc<dyn PluginExecutionContext>>,
    ) -> PluginInterface
    where
        R: BufRead + Send + 'static,
        W: Write + Send + 'static,
        E: PluginEncoder + 'static,
    {
        PluginInterfaceImpl::new((reader, encoder.clone()), (writer, encoder), context).into()
    }

    /// Write a [PluginCall] to the plugin.
    pub(crate) fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
        self.io.write_call(call)
    }

    /// Read a [PluginCallResponse] back from the plugin.
    pub(crate) fn read_call_response(&self) -> Result<PluginCallResponse, ShellError> {
        self.io.clone().read_call_response()
    }

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    pub(crate) fn make_pipeline_data(
        &self,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        self.io.clone().make_pipeline_data(header)
    }

    /// Create a valid header to send the given [`PipelineData`].
    ///
    /// Returns the header, and the arguments to be sent with `write_pipeline_data_stream`
    /// if necessary.
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    pub(crate) fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<
        (
            PipelineDataHeader,
            Option<(PipelineDataHeader, PipelineData)>,
        ),
        ShellError,
    > {
        self.io.make_pipeline_data_header(data)
    }

    /// Write the contents of a [PipelineData]. This is a no-op for non-stream data.
    #[track_caller]
    pub(crate) fn write_pipeline_data_stream(
        &self,
        header: &PipelineDataHeader,
        data: PipelineData,
    ) -> Result<(), ShellError> {
        self.io.write_pipeline_data_stream(header, data)
    }
}
