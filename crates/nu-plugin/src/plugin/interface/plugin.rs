//! Interface used by the engine to communicate with the plugin.

use std::{
    io::{BufRead, Write},
    sync::{atomic::AtomicUsize, Arc, Mutex},
};

use nu_protocol::{PipelineData, ShellError, Value};

use crate::{
    plugin::{context::PluginExecutionContext, PluginEncoder},
    protocol::{
        ExternalStreamInfo, PipelineDataHeader, PluginCall, PluginCallResponse, PluginCustomValue,
        PluginData, PluginInput, PluginOutput, RawStreamInfo, StreamData, StreamId,
    },
};

use super::{
    buffers::StreamBuffers,
    make_external_stream, make_list_stream,
    stream_data_io::{impl_stream_data_io, StreamDataIo, StreamDataIoExt},
    PluginRead, PluginWrite,
};

#[cfg(test)]
mod tests;

pub(crate) struct PluginInterfaceImpl<R, W> {
    // Always lock read and then write mutex, if using both
    // Stream inputs that can't be handled immediately can be put on the buffer
    read: Mutex<ReadPart<R>>,
    write: Mutex<W>,
    context: Option<Arc<dyn PluginExecutionContext>>,
    /// The next available stream id
    next_stream_id: AtomicUsize,
}

struct ReadPart<R> {
    reader: R,
    stream_buffers: StreamBuffers,
}

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
            }),
            write: Mutex::new(writer),
            context,
            next_stream_id: AtomicUsize::new(0),
        }
    }
}

impl<R> ReadPart<R> where R: PluginRead {
    fn handle_out_of_order(
        &mut self,
        _io: &Arc<PluginInterfaceImpl<R, impl PluginWrite>>,
        msg: PluginOutput
    ) -> Result<(), ShellError> {
        match msg {
            PluginOutput::CallResponse(_) => Err(ShellError::PluginFailedToDecode {
                msg: "unexpected CallResponse in this context".into()
            }),
            PluginOutput::StreamData(id, data) => self.stream_buffers.skip(id, data),
            PluginOutput::EngineCall(_, _) => todo!(),
        }
    }
}

// Implement the stream handling methods (see StreamDataIo).
impl_stream_data_io!(
    PluginInterfaceImpl,
    PluginOutput(read_output),
    PluginInput(write_input)
);

/// The trait indirection is so that we can hide the types with a trait object inside
/// PluginInterface. As such, this trait must remain object safe.
pub(crate) trait PluginInterfaceIo: StreamDataIo {
    fn context(&self) -> Option<&Arc<dyn PluginExecutionContext>>;
    fn write_call(&self, call: PluginCall) -> Result<(), ShellError>;
    fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError>;

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError>;

    /// Create a valid header to send the given PipelineData.
    ///
    /// Returns the header, and the PipelineData to be sent with `write_pipeline_data_stream`
    /// if necessary.
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<PipelineData>), ShellError>;
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
        let mut write = self.write.lock().expect("write mutex poisoned");
        log::trace!("Writing plugin call: {call:?}");

        write.write_input(&PluginInput::Call(call))?;
        write.flush()?;

        log::trace!("Wrote plugin call");
        Ok(())
    }

    fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError> {
        log::trace!("Reading plugin call response");

        loop {
            let mut read = self.read.lock().expect("read mutex poisoned");
            match read.reader.read_output()? {
                Some(PluginOutput::CallResponse(response)) => {
                    // Check the call input type to set the stream buffers up
                    if let PluginCallResponse::PipelineData(header) = &response {
                        read.stream_buffers.init_stream(header)?;
                    }
                    return Ok(response);
                }
                // Handle some other message
                Some(other) => read.handle_out_of_order(&self, other)?,
                // End of input
                None => {
                    return Err(ShellError::PluginFailedToDecode {
                        msg: "unexpected end of stream before receiving call response".into(),
                    })
                }
            }
        }
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
            PipelineDataHeader::ListStream(id) => {
                let ctrlc = context.ctrlc().cloned();
                Ok(make_list_stream(self, id, ctrlc))
            }
            PipelineDataHeader::ExternalStream(id, info) => {
                let ctrlc = context.ctrlc().cloned();
                Ok(make_external_stream(self, id, &info, ctrlc))
            }
        }
    }

    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<PipelineData>), ShellError> {
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
            PipelineData::ListStream(_, _) => Ok((
                PipelineDataHeader::ListStream(self.new_stream_id()?),
                Some(data),
            )),
            PipelineData::ExternalStream {
                span,
                ref stdout,
                ref stderr,
                ref exit_code,
                trim_end_newline,
                ..
            } => Ok((
                PipelineDataHeader::ExternalStream(
                    self.new_stream_id()?,
                    ExternalStreamInfo {
                        span,
                        stdout: stdout.as_ref().map(RawStreamInfo::from),
                        stderr: stderr.as_ref().map(RawStreamInfo::from),
                        has_exit_code: exit_code.is_some(),
                        trim_end_newline,
                    },
                ),
                Some(data),
            )),
            PipelineData::Empty => Ok((PipelineDataHeader::Empty, None)),
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

impl<T> From<Arc<T>> for PluginInterface where T: PluginInterfaceIo + 'static {
    fn from(value: Arc<T>) -> Self {
        PluginInterface {
            io: value
        }
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

    /// Create a valid header to send the given PipelineData.
    ///
    /// Returns the header, and the PipelineData to be sent with `write_pipeline_data_stream`
    /// if necessary.
    ///
    /// Error if [PluginExecutionContext] was not provided when creating the interface.
    pub(crate) fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<PipelineData>), ShellError> {
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
