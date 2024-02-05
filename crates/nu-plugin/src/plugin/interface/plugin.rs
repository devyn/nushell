//! Interface used by the engine to communicate with the plugin.

use std::{sync::{Mutex, Arc}, io::{BufRead, Write}, path::PathBuf};

use nu_protocol::{ShellError, Value, PipelineData, engine::EngineState, ast::Call, ListStream, RawStream};

use crate::{
    protocol::{
        PluginInput, PluginOutput, StreamData, PluginCallResponse, ExternalStreamInfo, PluginCall,
        PluginCustomValue
    },
    plugin::PluginEncoder,
};

use super::{
    stream_data_io::{impl_stream_data_io, StreamDataIo, StreamBuffers, StreamBuffer},
    make_list_stream,
    make_external_stream, write_full_list_stream, write_full_external_stream
};

/// The execution context of a plugin.
pub(crate) struct PluginExecutionContext {
    pub filename: PathBuf,
    pub shell: Option<PathBuf>,
    pub engine_state: EngineState,
    //pub stack: Stack,
    pub call: Call,
}

impl PluginExecutionContext {
    fn command_name(&self) -> &str {
        self.engine_state.get_decl(self.call.decl_id).name()
    }
}

struct PluginInterfaceImpl<R, W, E> {
    // Always lock read and then write mutex, if using both
    // Stream inputs that can't be handled immediately can be put on the buffer
    read: Mutex<(R, StreamBuffers)>,
    write: Mutex<W>,
    encoder: E,
    context: Option<Arc<PluginExecutionContext>>,
}

impl_stream_data_io!(PluginInterfaceImpl, PluginOutput (decode_output), PluginInput (encode_input));

// The trait indirection is so that we can hide the types with a trait object inside
/// EngineInterface. As such, this trait must remain object safe.
trait PluginInterfaceIo: StreamDataIo {
    fn context(&self) -> Option<&Arc<PluginExecutionContext>>;
    fn write_call(&self, call: PluginCall) -> Result<(), ShellError>;
    fn read_call_response(&self) -> Result<PluginCallResponse, ShellError>;
}

impl<R, W, E> PluginInterfaceIo for PluginInterfaceImpl<R, W, E>
where
    R: BufRead + Send,
    W: Write + Send,
    E: PluginEncoder + Send + Sync,
{
    fn context(&self) -> Option<&Arc<PluginExecutionContext>> {
        self.context.as_ref()
    }

    fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
        let mut write = self.write.lock().expect("write mutex poisoned");
        log::trace!("Writing plugin call: {call:?}");

        self.encoder.encode_input(&PluginInput::Call(call), &mut *write)?;

        write.flush().map_err(|err| {
            ShellError::GenericError {
                error: err.to_string(),
                msg: "failed to flush buffer".into(),
                span: None,
                help: None,
                inner: vec![]
            }
        })?;

        log::trace!("Wrote plugin call");
        Ok(())
    }

    fn read_call_response(&self) -> Result<PluginCallResponse, ShellError> {
        log::trace!("Reading plugin call response");

        let mut read = self.read.lock().expect("read mutex poisoned");
        loop {
            match self.encoder.decode_output(&mut read.0)? {
                Some(PluginOutput::CallResponse(response)) => {
                    // Check the call input type to set the stream buffers up
                    match &response {
                        PluginCallResponse::ListStream => {
                            read.1 = StreamBuffers::new_list();
                            log::trace!("Read plugin call response. Expecting list stream");
                        }
                        PluginCallResponse::ExternalStream(ExternalStreamInfo {
                            stdout,
                            stderr,
                            has_exit_code,
                            ..
                        }) => {
                            read.1 = StreamBuffers::new_external(
                                stdout.is_some(),
                                stderr.is_some(),
                                *has_exit_code
                            );
                            log::trace!("Read plugin call response. Expecting external stream");
                        }
                        _ => {
                            read.1 = StreamBuffers::default(); // no buffers
                            log::trace!("Read plugin call response. No stream expected");
                        }
                    }
                    return Ok(response);
                }
                // Skip over any remaining stream data for dropped streams
                Some(PluginOutput::StreamData(StreamData::List(_)))
                    if read.1.list.is_dropped() => continue,
                Some(PluginOutput::StreamData(StreamData::ExternalStdout(_)))
                    if read.1.external_stdout.is_dropped() => continue,
                Some(PluginOutput::StreamData(StreamData::ExternalStderr(_)))
                    if read.1.external_stderr.is_dropped() => continue,
                Some(PluginOutput::StreamData(StreamData::ExternalExitCode(_)))
                    if read.1.external_exit_code.is_dropped() => continue,
                // Other stream data is an error
                Some(PluginOutput::StreamData(_)) => return Err(ShellError::PluginFailedToDecode {
                    msg: "expected CallResponse, got unexpected StreamData".into()
                }),
                // End of input
                None => return Err(ShellError::PluginFailedToDecode {
                    msg: "unexpected end of stream before receiving call response".into()
                }),
            }
        }
    }
}

/// Implements communication and stream handling for a plugin instance.
#[derive(Clone)]
pub(crate) struct PluginInterface {
    io: Arc<dyn PluginInterfaceIo>,
    // FIXME: This is only necessary because trait upcasting is not yet supported, so we have to
    // generate this variant of the Arc while we know the actual type. It can be removed once
    // https://github.com/rust-lang/rust/issues/65991 is closed and released.
    io_stream: Arc<dyn StreamDataIo>,
}

impl PluginInterface {
    /// Create the plugin interface from the given reader, writer, encoder, and context.
    pub(crate) fn new<R, W, E>(
        reader: R,
        writer: W,
        encoder: E,
        context: Option<Arc<PluginExecutionContext>>,
    ) -> PluginInterface
    where
        R: BufRead + Send + 'static,
        W: Write + Send + 'static,
        E: PluginEncoder + Send + Sync + 'static,
    {
        let plugin_impl = PluginInterfaceImpl {
            read: Mutex::new((reader, StreamBuffers::default())),
            write: Mutex::new(writer),
            encoder,
            context,
        };
        let arc = Arc::new(plugin_impl);
        PluginInterface {
            io: arc.clone(),
            io_stream: arc
        }
    }

    /// Write a [PluginCall] to the plugin
    pub(crate) fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
        self.io.write_call(call)
    }

    /// Read a [PluginCallResponse] back from the plugin
    pub(crate) fn read_call_response(&self) -> Result<PluginCallResponse, ShellError> {
        self.io.read_call_response()
    }

    /// Create [PipelineData] appropriate for the given [PluginCallResponse].
    ///
    /// Only usable with response types that emulate [PipelineData].
    ///
    /// # Panics
    ///
    /// If [PluginExecutionContext] was not provided when creating the interface.
    pub(crate) fn make_pipeline_data(&self, response: PluginCallResponse)
        -> Result<PipelineData, ShellError>
    {
        let context = self.io.context()
            .expect("PluginExecutionContext must be provided to call make_pipeline_data");

        match response {
            PluginCallResponse::Error(err) =>
                Err(err.into()),
            PluginCallResponse::Signature(_) =>
                Err(ShellError::GenericError {
                    error: "Plugin missing value".into(),
                    msg: "Received a signature from plugin instead of value or stream".into(),
                    span: Some(context.call.head),
                    help: None,
                    inner: vec![],
                }),
            PluginCallResponse::Empty =>
                Ok(PipelineData::Empty),
            PluginCallResponse::Value(value) =>
                Ok(PipelineData::Value(*value, None)),
            PluginCallResponse::PluginData(name, plugin_data) => {
                // Convert to PluginCustomData
                let value = Value::custom_value(
                    Box::new(PluginCustomValue {
                        name,
                        data: plugin_data.data,
                        filename: context.filename.clone(),
                        shell: context.shell.clone(),
                        source: context.command_name().to_owned(),
                    }),
                    plugin_data.span
                );
                Ok(PipelineData::Value(value, None))
            }
            PluginCallResponse::ListStream =>
                Ok(make_list_stream(self.io_stream.clone(), context.engine_state.ctrlc.clone())),
            PluginCallResponse::ExternalStream(info) =>
                Ok(make_external_stream(
                    self.io_stream.clone(),
                    &info,
                    context.engine_state.ctrlc.clone()
                )),
        }
    }

    /// Write the contents of a [ListStream] to `io`.
    pub fn write_full_list_stream(&self, list_stream: ListStream) -> Result<(), ShellError> {
        write_full_list_stream(&self.io_stream, list_stream)
    }

    /// Write the contents of a [PipelineData::ExternalStream].
    pub fn write_full_external_stream(
        &self,
        stdout: Option<RawStream>,
        stderr: Option<RawStream>,
        exit_code: Option<ListStream>
    ) -> Result<(), ShellError> {
        write_full_external_stream(&self.io_stream, stdout, stderr, exit_code)
    }
}
