//! Implements the stream multiplexing interface for both the plugin side and the engine side.

use std::{sync::{Arc, atomic::AtomicBool}, path::Path};

use nu_protocol::{ShellError, Value, Span, ListStream, PipelineData, RawStream};

use crate::protocol::{ExternalStreamInfo, CallInput, PluginData, PluginCustomValue, RawStreamInfo};

mod stream_data_io;
use stream_data_io::*;

mod engine;
pub use engine::EngineInterface;

mod plugin;
pub(crate) use plugin::{PluginInterface, PluginExecutionContext};

// Non-fused iterator: should call .fuse()
struct PluginListStream {
    io: Arc<dyn StreamDataIo>,
}

impl Iterator for PluginListStream {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        match self.io.read_list() {
            Ok(value) => value,
            Err(err) => Some(Value::error(err, Span::unknown()))
        }
    }
}

impl Drop for PluginListStream {
    fn drop(&mut self) {
        self.io.drop_list();
    }
}

fn make_list_stream(
    source: Arc<dyn StreamDataIo>,
    ctrlc: Option<Arc<AtomicBool>>
) -> PipelineData {
    PipelineData::ListStream(
        ListStream::from_stream(PluginListStream { io: source }.fuse(), ctrlc),
        None
    )
}

/// Write the contents of a [ListStream] to `io`.
fn write_full_list_stream(io: &Arc<dyn StreamDataIo>, list_stream: ListStream)
    -> Result<(), ShellError>
{
    // Consume the stream and write it via StreamDataIo.
    for value in list_stream {
        io.write_list(Some(match value {
            Value::LazyRecord { val, .. } => val.collect()?,
            _ => value
        }))?;
    }
    // End of stream
    io.write_list(None)
}

// Non-fused iterator: should call .fuse()
struct PluginExternalStdoutStream {
    io: Arc<dyn StreamDataIo>,
}

impl Iterator for PluginExternalStdoutStream
{
    type Item = Result<Vec<u8>, ShellError>;

    fn next(&mut self) -> Option<Result<Vec<u8>, ShellError>> {
        self.io.read_external_stdout().transpose()
    }
}

impl Drop for PluginExternalStdoutStream {
    fn drop(&mut self) {
        self.io.drop_external_stdout();
    }
}

// Non-fused iterator: should call .fuse()
struct PluginExternalStderrStream {
    io: Arc<dyn StreamDataIo>,
}

impl Iterator for PluginExternalStderrStream
{
    type Item = Result<Vec<u8>, ShellError>;

    fn next(&mut self) -> Option<Result<Vec<u8>, ShellError>> {
        self.io.read_external_stderr().transpose()
    }
}

impl Drop for PluginExternalStderrStream {
    fn drop(&mut self) {
        self.io.drop_external_stderr();
    }
}

// Non-fused iterator: should call .fuse()
struct PluginExternalExitCodeStream {
    io: Arc<dyn StreamDataIo>,
}

impl Iterator for PluginExternalExitCodeStream
{
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        match self.io.read_external_exit_code() {
            Ok(value) => value,
            Err(err) => Some(Value::error(err, Span::unknown()))
        }
    }
}

impl Drop for PluginExternalExitCodeStream {
    fn drop(&mut self) {
        self.io.drop_external_exit_code();
    }
}

fn make_external_stream(
    source: Arc<dyn StreamDataIo>,
    info: &ExternalStreamInfo,
    ctrlc: Option<Arc<AtomicBool>>
) -> PipelineData {
    PipelineData::ExternalStream {
        stdout: info.stdout.as_ref().map(|stdout_info| {
            let stream = PluginExternalStdoutStream {
                io: source.clone()
            }.fuse();
            RawStream::new(Box::new(stream), ctrlc.clone(), info.span, stdout_info.known_size)
        }),
        stderr: info.stderr.as_ref().map(|stderr_info| {
            let stream = PluginExternalStderrStream {
                io: source.clone()
            }.fuse();
            RawStream::new(Box::new(stream), ctrlc.clone(), info.span, stderr_info.known_size)
        }),
        exit_code: info.has_exit_code.then(|| {
            ListStream::from_stream(PluginExternalExitCodeStream {
                io: source.clone()
            }.fuse(), ctrlc.clone())
        }),
        span: info.span,
        metadata: None,
        trim_end_newline: info.trim_end_newline,
    }
}

/// Write the contents of a [PipelineData::ExternalStream] to `io`.
fn write_full_external_stream(
    io: &Arc<dyn StreamDataIo>,
    stdout: Option<RawStream>,
    stderr: Option<RawStream>,
    exit_code: Option<ListStream>
) -> Result<(), ShellError> {
    // Consume all streams simultaneously by launching three threads
    for thread in [
        stdout.map(|stdout| {
            let io = io.clone();
            std::thread::spawn(move || {
                for bytes in stdout.stream {
                    io.write_external_stdout(Some(bytes))?;
                }
                io.write_external_stdout(None)
            })
        }),
        stderr.map(|stderr| {
            let io = io.clone();
            std::thread::spawn(move || {
                for bytes in stderr.stream {
                    io.write_external_stderr(Some(bytes))?;
                }
                io.write_external_stderr(None)
            })
        }),
        exit_code.map(|exit_code| {
            let io = io.clone();
            std::thread::spawn(move || {
                for value in exit_code {
                    io.write_external_exit_code(Some(value))?;
                }
                io.write_external_exit_code(None)
            })
        }),
    ] {
        if let Some(thread) = thread {
            thread.join().expect("stream consumer thread panicked")?;
        }
    }
    Ok(())
}

/// Prepare [CallInput] for [PipelineData].
///
/// Handles converting [PluginCustomValue] to [CallInput::Data] if the `plugin_filename` is correct.
///
/// Does not actually send any stream data. You still need to call either [write_full_list_stream]
/// or [write_full_external_stream] as appropriate.
pub(crate) fn make_call_input_from_pipeline_data(
    input: &PipelineData,
    plugin_name: &str,
    plugin_filename: &Path,
) -> Result<CallInput, ShellError> {
    match *input {
        PipelineData::Value(ref value @ Value::CustomValue { ref val, .. }, _) => {
            match val.as_any().downcast_ref::<PluginCustomValue>() {
                Some(plugin_data) if plugin_data.filename == plugin_filename => {
                    Ok(CallInput::Data(PluginData {
                        data: plugin_data.data.clone(),
                        span: value.span(),
                    }))
                }
                _ => {
                    let custom_value_name = val.value_string();
                    Err(ShellError::GenericError {
                        error: format!(
                            "Plugin {} can not handle the custom value {}",
                            plugin_name, custom_value_name
                        ),
                        msg: format!("custom value {custom_value_name}"),
                        span: Some(value.span()),
                        help: None,
                        inner: vec![],
                    })
                }
            }
        }
        PipelineData::Value(Value::LazyRecord { ref val, .. }, _) =>
            Ok(CallInput::Value(val.collect()?)),
        PipelineData::Value(ref value, _) => Ok(CallInput::Value(value.clone())),
        PipelineData::ListStream(_, _) => Ok(CallInput::ListStream),
        PipelineData::ExternalStream {
            span,
            ref stdout,
            ref stderr,
            ref exit_code,
            trim_end_newline,
            ..
        } => {
            Ok(CallInput::ExternalStream(ExternalStreamInfo {
                span,
                stdout: stdout.as_ref().map(RawStreamInfo::from),
                stderr: stderr.as_ref().map(RawStreamInfo::from),
                has_exit_code: exit_code.is_some(),
                trim_end_newline,
            }))
        }
        PipelineData::Empty => Ok(CallInput::Empty),
    }
}
