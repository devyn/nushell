//! Interface used by the plugin to communicate with the engine.

use std::{
    io::{BufRead, Write},
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc, Mutex, MutexGuard},
};

use nu_protocol::{engine::Closure, Config, CustomValue, PipelineData, ShellError, Spanned, Value};

use crate::{
    plugin::PluginEncoder,
    protocol::{
        CallInfo, EngineCall, EngineCallId, EngineCallResponse, ExternalStreamInfo, ListStreamInfo,
        PipelineDataHeader, PluginCall, PluginCallResponse, PluginData, PluginInput, PluginOutput,
        RawStreamInfo, StreamId, StreamMessage,
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

#[derive(Debug)]
pub(crate) struct EngineInterfaceImpl<R, W> {
    // Always lock read and then write mutex, if using both
    read: Mutex<ReadPart<R, W>>,
    write: Mutex<WritePart<W>>,
    /// The next available stream id
    next_stream_id: AtomicUsize,
    /// The next available engine call id
    next_engine_call_id: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct ReadPart<R, W> {
    reader: R,
    /// Stores stream messages that can't be handled immediately
    stream_buffers: StreamBuffers,
    /// Lists engine calls that are expecting a response. If a response is received out of order
    /// it can be placed in here for the waiting thread to pick up later
    pending_engine_calls: Vec<(EngineCallId, Option<EngineCallResponse>)>,
    /// Just to preserve the writer type
    write_marker: PhantomData<W>,
}

#[derive(Debug)]
pub(crate) struct WritePart<W>(W);

impl<R, W> EngineInterfaceImpl<R, W> {
    pub(crate) fn new(reader: R, writer: W) -> EngineInterfaceImpl<R, W> {
        EngineInterfaceImpl {
            read: Mutex::new(ReadPart {
                reader,
                stream_buffers: StreamBuffers::default(),
                pending_engine_calls: vec![],
                write_marker: PhantomData,
            }),
            write: Mutex::new(WritePart(writer)),
            next_stream_id: AtomicUsize::new(0),
            next_engine_call_id: AtomicUsize::new(0),
        }
    }
}

impl<R, W> StreamDataIoBase for EngineInterfaceImpl<R, W>
where
    R: PluginRead,
    W: PluginWrite,
{
    type ReadPart = ReadPart<R, W>;
    type WritePart = WritePart<W>;

    fn lock_read(&self) -> Result<MutexGuard<ReadPart<R, W>>, ShellError> {
        self.read.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in EngineInterface: read mutex poisoned due to panic".into(),
        })
    }

    fn lock_write(&self) -> Result<MutexGuard<WritePart<W>>, ShellError> {
        self.write.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in EngineInterface: write mutex poisoned due to panic".into(),
        })
    }

    fn new_stream_id(&self) -> Result<StreamId, ShellError> {
        next_id_from(&self.next_stream_id)
    }
}

impl<R, W> StreamDataRead for ReadPart<R, W>
where
    R: PluginRead,
    W: PluginWrite,
{
    type Message = PluginInput;
    type Base = EngineInterfaceImpl<R, W>;

    fn read(&mut self) -> Result<Option<Self::Message>, ShellError> {
        self.reader.read_input()
    }

    fn stream_buffers(&mut self) -> &mut StreamBuffers {
        &mut self.stream_buffers
    }

    // Default message handler
    fn handle_message(
        &mut self,
        _io: &Arc<Self::Base>,
        msg: PluginInput,
    ) -> Result<(), ShellError> {
        match msg {
            PluginInput::Call(_) => Err(ShellError::PluginFailedToDecode {
                msg: "unexpected Call in this context - possibly nested".into(),
            }),
            // Handle out of order stream messages
            PluginInput::StreamData(id, data) => {
                self.handle_out_of_order(StreamMessage::Data(id, data))
            }
            // Store engine call responses in pending_engine_calls. If the call response will
            // produce a stream, that also must be initialized now so we can store those stream
            // messages.
            PluginInput::EngineCallResponse(id, response) => {
                // Find the matching engine call response hole, or error if it wasn't registered
                let (_, response_hole) = self
                    .pending_engine_calls
                    .iter_mut()
                    .find(|(call_id, _)| id == *call_id)
                    .ok_or_else(|| ShellError::PluginFailedToDecode {
                        msg: format!("unexpected EngineCallResponse id={id}"),
                    })?;

                if response_hole.is_none() {
                    // We may have to set up a stream
                    if let EngineCallResponse::PipelineData(header) = &response {
                        self.stream_buffers.init_stream(header)?;
                    }
                    *response_hole = Some(response);
                    Ok(())
                } else {
                    Err(ShellError::PluginFailedToDecode {
                        msg: format!("received multiple responses for engine call id={id}"),
                    })
                }
            }
        }
    }
}

impl<W> StreamDataWrite for WritePart<W>
where
    W: PluginWrite,
{
    type Message = PluginOutput;

    fn write(&mut self, msg: Self::Message) -> Result<(), ShellError> {
        self.0.write_output(&msg)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        self.0.flush()
    }
}

/// The trait indirection is so that we can hide the types with a trait object inside
/// EngineInterface. As such, this trait must remain object safe.
pub(crate) trait EngineInterfaceIo: StreamDataIo {
    fn read_call(self: Arc<Self>) -> Result<Option<PluginCall>, ShellError>;
    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError>;

    /// Write an [EngineCall] and return the [EngineCallId] it was registered with.
    fn write_engine_call(&self, call: EngineCall) -> Result<EngineCallId, ShellError>;

    /// Read the [EngineCallResponse] corresponding to `id`.
    ///
    /// Panics if the `id` was not one previously returned by `write_engine_call` or if it was
    /// already read before.
    fn read_engine_call_response(
        self: Arc<Self>,
        id: EngineCallId,
    ) -> Result<EngineCallResponse, ShellError>;

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError>;

    /// Create a valid header to send the given [`PipelineData`].
    ///
    /// Returns the header, and the arguments to be sent to `write_pipeline_data_stream`
    /// if necessary.
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
}

impl<R, W> EngineInterfaceIo for EngineInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn read_call(self: Arc<Self>) -> Result<Option<PluginCall>, ShellError> {
        loop {
            let mut read = self.lock_read()?;
            match read.read()? {
                Some(PluginInput::Call(call)) => {
                    // Check the call input type to set the stream buffers up
                    if let PluginCall::Run(CallInfo { ref input, .. }) = call {
                        read.stream_buffers.init_stream(input)?;
                    }
                    return Ok(Some(call));
                }
                // Handle some other message
                Some(other) => read.handle_message(&self, other)?,
                // End of input
                None => return Ok(None),
            }
        }
    }

    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError> {
        let mut write = self.lock_write()?;

        write.write(PluginOutput::CallResponse(response))?;
        write.flush()
    }

    fn write_engine_call(&self, call: EngineCall) -> Result<EngineCallId, ShellError> {
        // Get a new id for the engine call
        let id = next_id_from(&self.next_engine_call_id)?;

        // Register the pending engine call
        let mut read = self.lock_read()?;
        read.pending_engine_calls.push((id, None));
        drop(read);

        // Send the call
        let mut write = self.lock_write()?;

        write.write(PluginOutput::EngineCall(id, call))?;
        write.flush()?;

        Ok(id)
    }

    fn read_engine_call_response(
        self: Arc<Self>,
        id: EngineCallId,
    ) -> Result<EngineCallResponse, ShellError> {
        loop {
            let mut read = self.lock_read()?;

            let pending_index = read
                .pending_engine_calls
                .iter()
                .position(|(reg_id, _)| id == *reg_id)
                .ok_or_else(|| ShellError::NushellFailed {
                    msg: "engine call not found in pending_engine_calls".into(),
                })?;

            // Check if another thread already read our response
            if read.pending_engine_calls[pending_index].1.is_some() {
                // It's already been received. The other thread will have already set up the stream
                // so we just need to return it.
                return Ok(read
                    .pending_engine_calls
                    .swap_remove(pending_index)
                    .1
                    .unwrap());
            }

            // Otherwise read and hope that we get the response
            match read.read()? {
                Some(PluginInput::EngineCallResponse(resp_id, response)) if id == resp_id => {
                    // Remove the pending call
                    read.pending_engine_calls.swap_remove(pending_index);
                    // Initialize stream, if necessary
                    if let EngineCallResponse::PipelineData(header) = &response {
                        read.stream_buffers.init_stream(header)?;
                    }
                    return Ok(response);
                }
                // Handle some other message
                Some(other) => read.handle_message(&self, other)?,
                // End of input
                None => {
                    return Err(ShellError::PluginFailedToDecode {
                        msg: "unexpected end of input".into(),
                    })
                }
            }
        }
    }

    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        match header {
            PipelineDataHeader::Empty => Ok(PipelineData::Empty),
            PipelineDataHeader::Value(value) => Ok(PipelineData::Value(value, None)),
            PipelineDataHeader::PluginData(plugin_data) => {
                // Deserialize custom value
                bincode::deserialize::<Box<dyn CustomValue>>(&plugin_data.data)
                    .map(|custom_value| {
                        let value = Value::custom_value(custom_value, plugin_data.span);
                        PipelineData::Value(value, None)
                    })
                    .map_err(|err| ShellError::PluginFailedToDecode {
                        msg: err.to_string(),
                    })
            }
            PipelineDataHeader::ListStream(info) => Ok(make_pipe_list_stream(self, &info, None)),
            PipelineDataHeader::ExternalStream(info) => {
                Ok(make_pipe_external_stream(self, &info, None))
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
        match data {
            PipelineData::Value(value, _) => {
                let span = value.span();
                match value {
                    // Serialize custom values as PluginData
                    Value::CustomValue { val, .. } => match bincode::serialize(&val) {
                        Ok(data) => {
                            let name = Some(val.value_string());
                            Ok((
                                PipelineDataHeader::PluginData(PluginData { name, data, span }),
                                None,
                            ))
                        }
                        Err(err) => {
                            return Err(ShellError::PluginFailedToEncode {
                                msg: err.to_string(),
                            })
                        }
                    },
                    // Other values can be serialized as-is
                    value => Ok((PipelineDataHeader::Value(value), None)),
                }
            }
            PipelineData::ListStream(..) => {
                let header = PipelineDataHeader::ListStream(ListStreamInfo {
                    id: self.new_stream_id()?,
                });
                Ok((header.clone(), Some((header, data))))
            }
            PipelineData::ExternalStream {
                ref stdout,
                ref stderr,
                ref exit_code,
                span,
                trim_end_newline,
                ..
            } => {
                // Gather info from the stream
                let info = ExternalStreamInfo {
                    span,
                    stdout: if let Some(stdout) = stdout {
                        Some(RawStreamInfo::new(self.new_stream_id()?, stdout))
                    } else {
                        None
                    },
                    stderr: if let Some(stderr) = stderr {
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
                };
                let header = PipelineDataHeader::ExternalStream(info);
                Ok((header.clone(), Some((header, data))))
            }
            PipelineData::Empty => Ok((PipelineDataHeader::Empty, None)),
        }
    }
}

/// A reference through which the nushell engine can be interacted with during execution.
#[derive(Clone)]
pub struct EngineInterface {
    io: Arc<dyn EngineInterfaceIo>,
}

impl<R, W> From<EngineInterfaceImpl<R, W>> for EngineInterface
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn from(engine_impl: EngineInterfaceImpl<R, W>) -> Self {
        Arc::new(engine_impl).into()
    }
}

impl<T> From<Arc<T>> for EngineInterface
where
    T: EngineInterfaceIo + 'static,
{
    fn from(value: Arc<T>) -> Self {
        EngineInterface { io: value }
    }
}

impl EngineInterface {
    /// Create the engine interface from the given reader, writer, and encoder.
    pub(crate) fn new<R, W, E>(reader: R, writer: W, encoder: E) -> EngineInterface
    where
        R: BufRead + Send + 'static,
        W: Write + Send + 'static,
        E: PluginEncoder + 'static,
    {
        EngineInterfaceImpl::new((reader, encoder.clone()), (writer, encoder)).into()
    }

    /// Read a plugin call from the engine
    pub(crate) fn read_call(&self) -> Result<Option<PluginCall>, ShellError> {
        self.io.clone().read_call()
    }

    /// Create [PipelineData] appropriate for the given [PipelineDataHeader].
    pub(crate) fn make_pipeline_data(
        &self,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        self.io.clone().make_pipeline_data(header)
    }

    /// Write a plugin call response back to the engine
    pub(crate) fn write_call_response(
        &self,
        response: PluginCallResponse,
    ) -> Result<(), ShellError> {
        self.io.write_call_response(response)
    }

    /// Write a response appropriate for the given [PipelineData] and consume the stream(s) to
    /// completion, if any.
    pub(crate) fn write_pipeline_data_response(
        &self,
        data: PipelineData,
    ) -> Result<(), ShellError> {
        let (header, rest) = self.io.make_pipeline_data_header(data)?;

        self.io
            .write_call_response(PluginCallResponse::PipelineData(header))?;

        if let Some((header, data)) = rest {
            self.io.write_pipeline_data_stream(&header, data)
        } else {
            Ok(())
        }
    }

    /// Get the full shell configuration from the engine. As this is quite a large object, it is
    /// provided on request only.
    ///
    /// # Example
    ///
    /// Format a value in the user's preferred way:
    ///
    /// ```
    /// # use nu_protocol::{Value, ShellError};
    /// # use nu_plugin::EngineInterface;
    /// # fn example(engine: &EngineInterface, value: &Value) -> Result<(), ShellError> {
    /// let config = engine.get_config()?;
    /// eprintln!("{}", value.into_string(", ", &config));
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_config(&self) -> Result<Box<Config>, ShellError> {
        let call_id = self.io.write_engine_call(EngineCall::GetConfig)?;
        match self.io.clone().read_engine_call_response(call_id)? {
            EngineCallResponse::Config(config) => Ok(config),
            EngineCallResponse::Error(err) => Err(err),
            _ => Err(ShellError::PluginFailedToDecode {
                msg: "received unexpected response for EngineCall::GetConfig".into(),
            }),
        }
    }

    /// Ask the engine to evaluate a closure. Input to the closure is passed as a stream, and the
    /// output is available as a stream.
    ///
    /// Set `redirect_stdout` to `true` to capture the standard output stream of an external
    /// command, if the closure results in an external command.
    ///
    /// Set `redirect_stderr` to `true` to capture the standard error stream of an external command,
    /// if the closure results in an external command.
    ///
    /// # Example
    ///
    /// Invoked as:
    ///
    /// ```nushell
    /// my_command { seq 1 $in | each { |n| $"Hello, ($n)" } }
    /// ```
    ///
    /// ```
    /// # use nu_protocol::{Value, ShellError, PipelineData};
    /// # use nu_plugin::{EngineInterface, EvaluatedCall};
    /// # fn example(engine: &EngineInterface, call: &EvaluatedCall) -> Result<(), ShellError> {
    /// let closure = call.req(0)?;
    /// let input = PipelineData::Value(Value::int(4, call.head), None);
    /// let output = engine.eval_closure_with_stream(
    ///     &closure,
    ///     vec![],
    ///     input,
    ///     true,
    ///     false,
    /// )?;
    /// for value in output {
    ///     eprintln!("Closure says: {}", value.as_string()?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// Closure says: Hello, 1
    /// Closure says: Hello, 2
    /// Closure says: Hello, 3
    /// Closure says: Hello, 4
    /// ```
    pub fn eval_closure_with_stream(
        &self,
        closure: &Spanned<Closure>,
        positional: Vec<Value>,
        input: PipelineData,
        redirect_stdout: bool,
        redirect_stderr: bool,
    ) -> Result<PipelineData, ShellError> {
        let (input_header, input_stream) = self.io.make_pipeline_data_header(input)?;

        let call = EngineCall::EvalClosure {
            closure: closure.clone(),
            positional,
            input: input_header.clone(),
            redirect_stdout,
            redirect_stderr,
        };

        // Write call and get id
        let call_id = self.io.write_engine_call(call)?;

        // Send input if necessary
        if let Some((header, data)) = input_stream {
            self.io.write_pipeline_data_stream(&header, data)?;
        }

        // Wait for response
        match self.io.clone().read_engine_call_response(call_id)? {
            EngineCallResponse::PipelineData(header) => {
                // Generate PipelineData (possibly stream) according to the header returned
                self.io.clone().make_pipeline_data(header)
            }
            EngineCallResponse::Error(err) => Err(err),
            _ => Err(ShellError::PluginFailedToDecode {
                msg: "unexpected response to EngineCall::EvalClosure".into(),
            }),
        }
    }

    /// Ask the engine to evaluate a closure. Input is optionally passed as a [`Value`], and output
    /// of the closure is collected to a [`Value`] even if it is a stream.
    ///
    /// If the closure results in an external command, the return value will be a collected string
    /// or binary value of the standard output stream of that command, similar to calling
    /// [`eval_closure_with_stream`] with `redirect_stdout` = `true` and `redirect_stderr` =
    /// `false`.
    ///
    /// Use [`eval_closure_with_stream`] if more control over the input and output is desired.
    ///
    /// # Example
    ///
    /// Invoked as:
    ///
    /// ```nushell
    /// my_command { |number| $number + 1}
    /// ```
    ///
    /// ```
    /// # use nu_protocol::{Value, ShellError};
    /// # use nu_plugin::{EngineInterface, EvaluatedCall};
    /// # fn example(engine: &EngineInterface, call: &EvaluatedCall) -> Result<(), ShellError> {
    /// let closure = call.req(0)?;
    /// for n in 0..4 {
    ///     let result = engine.eval_closure(&closure, vec![Value::int(n, call.head)], None)?;
    ///     eprintln!("{} => {}", n, result.as_int()?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Output:
    ///
    /// ```text
    /// 0 => 1
    /// 1 => 2
    /// 2 => 3
    /// 3 => 4
    /// ```
    pub fn eval_closure(
        &self,
        closure: &Spanned<Closure>,
        positional: Vec<Value>,
        input: Option<Value>,
    ) -> Result<Value, ShellError> {
        let input = input.map_or_else(|| PipelineData::Empty, |v| PipelineData::Value(v, None));
        let output = self.eval_closure_with_stream(closure, positional, input, true, false)?;
        // Unwrap an error value
        match output.into_value(closure.span) {
            Value::Error { error, .. } => Err(*error),
            value => Ok(value),
        }
    }
}
