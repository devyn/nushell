//! Interface used by the plugin to communicate with the engine.

use std::{sync::{Mutex, Arc}, io::{BufRead, Write}};

use nu_protocol::{ShellError, Value, PipelineData, CustomValue};

use crate::{
    protocol::{
        PluginCall, PluginCallResponse, PluginInput, PluginOutput, StreamData, CallInput, CallInfo,
        ExternalStreamInfo, PluginData, RawStreamInfo
    },
    plugin::PluginEncoder,
};

use super::{
    stream_data_io::{impl_stream_data_io, StreamDataIo, StreamBuffers, StreamBuffer},
    make_list_stream,
    make_external_stream,
    write_full_external_stream,
    write_full_list_stream
};

pub struct EngineInterfaceImpl<R, W, E> {
    // Always lock read and then write mutex, if using both
    // Stream inputs that can't be handled immediately can be put on the buffer
    read: Mutex<(R, StreamBuffers)>,
    write: Mutex<W>,
    encoder: E,
}

// The trait indirection is so that we can hide the types with a trait object inside
/// EngineInterface. As such, this trait must remain object safe.
pub(crate) trait EngineInterfaceIo: StreamDataIo {
    fn read_call(&self) -> Result<Option<PluginCall>, ShellError>;
    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError>;
}

impl_stream_data_io!(EngineInterfaceImpl, PluginInput (decode_input), PluginOutput (encode_output));

impl<R, W, E> EngineInterfaceIo for EngineInterfaceImpl<R, W, E>
where
    R: BufRead + Send,
    W: Write + Send,
    E: PluginEncoder,
{
    fn read_call(&self) -> Result<Option<PluginCall>, ShellError> {
        let mut read = self.read.lock().expect("read mutex poisoned");
        loop {
            let input = self.encoder.decode_input(&mut read.0)?;
            match input {
                Some(PluginInput::Call(call)) => {
                    // Check the call input type to set the stream buffers up
                    match &call {
                        PluginCall::Run(CallInfo { input: CallInput::ListStream, .. }) => {
                            read.1 = StreamBuffers::new_list();
                        }
                        PluginCall::Run(CallInfo {
                            input: CallInput::ExternalStream(ExternalStreamInfo {
                                stdout,
                                stderr,
                                has_exit_code,
                                ..
                            }),
                            ..
                        }) => {
                            read.1 = StreamBuffers::new_external(
                                stdout.is_some(),
                                stderr.is_some(),
                                *has_exit_code
                            );
                        }
                        _ => {
                            read.1 = StreamBuffers::default(); // no buffers
                        }
                    }
                    return Ok(Some(call));
                }
                // Skip over any remaining stream data for dropped streams
                Some(PluginInput::StreamData(StreamData::List(_)))
                    if read.1.list.is_dropped() => continue,
                Some(PluginInput::StreamData(StreamData::ExternalStdout(_)))
                    if read.1.external_stdout.is_dropped() => continue,
                Some(PluginInput::StreamData(StreamData::ExternalStderr(_)))
                    if read.1.external_stderr.is_dropped() => continue,
                Some(PluginInput::StreamData(StreamData::ExternalExitCode(_)))
                    if read.1.external_exit_code.is_dropped() => continue,
                // Other stream data is an error
                Some(PluginInput::StreamData(_)) => return Err(ShellError::PluginFailedToDecode {
                    msg: "expected Call, got unexpected StreamData".into()
                }),
                // End of input
                None => return Ok(None),
            }
        }
    }

    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError> {
        let mut write = self.write.lock().expect("write mutex poisoned");

        self.encoder.encode_output(
            &PluginOutput::CallResponse(response), &mut *write)?;

        write.flush().map_err(|err| {
            ShellError::GenericError {
                error: err.to_string(),
                msg: "failed to flush buffer".into(),
                span: None,
                help: None,
                inner: vec![]
            }
        })
    }
}

/// A reference through which the nushell engine can be interacted with during execution.
#[derive(Clone)]
pub struct EngineInterface {
    io: Arc<dyn EngineInterfaceIo>,
    // FIXME: This is only necessary because trait upcasting is not yet supported, so we have to
    // generate this variant of the Arc while we know the actual type. It can be removed once
    // https://github.com/rust-lang/rust/issues/65991 is closed and released.
    io_stream: Arc<dyn StreamDataIo>,
}

impl EngineInterface {
    /// Create the engine interface from the given reader, writer, and encoder.
    pub(crate) fn new<R, W, E>(reader: R, writer: W, encoder: E) -> EngineInterface
    where
        R: BufRead + Send + 'static,
        W: Write + Send + 'static,
        E: PluginEncoder + 'static,
    {
        let engine_impl = EngineInterfaceImpl {
            read: Mutex::new((reader, StreamBuffers::default())),
            write: Mutex::new(writer),
            encoder
        };
        let arc = Arc::new(engine_impl);
        EngineInterface {
            io: arc.clone(),
            io_stream: arc
        }
    }

    /// Read a plugin call from the engine
    pub(crate) fn read_call(&self) -> Result<Option<PluginCall>, ShellError> {
        self.io.read_call()
    }

    /// Create [PipelineData] appropriate for the given [CallInput].
    ///
    /// Will cause a panic unless called after `read_call` returns [PluginCall::Run]
    pub(crate) fn make_pipeline_data(&self, call_input: CallInput)
        -> Result<PipelineData, ShellError>
    {
        match call_input {
            CallInput::Empty =>
                Ok(PipelineData::Empty),
            CallInput::Value(value) =>
                Ok(PipelineData::Value(value, None)),
            CallInput::Data(plugin_data) => {
                // Deserialize custom value
                bincode::deserialize::<Box<dyn CustomValue>>(&plugin_data.data)
                    .map(|custom_value| {
                        let value = Value::custom_value(custom_value, plugin_data.span);
                        PipelineData::Value(value, None)
                    })
                    .map_err(|err| ShellError::PluginFailedToDecode {
                        msg: err.to_string(),
                    })
            },
            CallInput::ListStream =>
                Ok(make_list_stream(self.io_stream.clone(), None)),
            CallInput::ExternalStream(info) =>
                Ok(make_external_stream(self.io_stream.clone(), &info, None)),
        }
    }

    /// Write a plugin call response back to the engine
    pub(crate) fn write_call_response(&self, response: PluginCallResponse)
        -> Result<(), ShellError>
    {
        self.io.write_call_response(response)
    }

    /// Write a response appropriate for the given [PipelineData] and consume the stream(s) to
    /// completion, if any.
    pub(crate) fn write_pipeline_data_response(&self, data: PipelineData)
        -> Result<(), ShellError>
    {
        match data {
            PipelineData::Value(value, _) => {
                let span = value.span();
                let response = match value {
                    // Serialize custom values as PluginData
                    Value::CustomValue { val, .. } => match bincode::serialize(&val) {
                        Ok(data) => {
                            let name = val.value_string();
                            PluginCallResponse::PluginData(name, PluginData { data, span })
                        }
                        Err(err) => return Err(ShellError::PluginFailedToEncode {
                            msg: err.to_string(),
                        })
                    },
                    // Other values can be serialized as-is
                    value => PluginCallResponse::Value(Box::new(value))
                };
                // Simple response, no stream.
                self.write_call_response(response)
            }
            PipelineData::ListStream(stream, _) => {
                self.write_call_response(PluginCallResponse::ListStream)?;
                write_full_list_stream(&self.io_stream, stream)
            }
            PipelineData::ExternalStream { stdout, stderr, exit_code, span, trim_end_newline, .. } => {
                // Gather info from the stream
                let info = ExternalStreamInfo {
                    span,
                    stdout: stdout.as_ref().map(RawStreamInfo::from),
                    stderr: stderr.as_ref().map(RawStreamInfo::from),
                    has_exit_code: exit_code.is_some(),
                    trim_end_newline,
                };
                self.write_call_response(PluginCallResponse::ExternalStream(info))?;
                write_full_external_stream(&self.io_stream, stdout, stderr, exit_code)
            }
            PipelineData::Empty => {
                self.write_call_response(PluginCallResponse::Value(Default::default()))
            }
        }
    }
}
