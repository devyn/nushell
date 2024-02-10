//! Implements the stream multiplexing interface for both the plugin side and the engine side.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    Arc,
};

use nu_protocol::{ListStream, PipelineData, RawStream, ShellError, Span, Value};

use crate::{
    plugin::PluginEncoder,
    protocol::{ExternalStreamInfo, PluginInput, PluginOutput, StreamId},
};

mod buffers;

mod stream_data_io;
pub(crate) use stream_data_io::StreamDataIo;

mod engine;
pub use engine::EngineInterface;

mod plugin;
pub(crate) use plugin::PluginInterface;

#[cfg(test)]
mod test_util;

/// Read [PluginInput] or [PluginOutput] from the stream.
///
/// This abstraction is really only used to make testing easier; in general this will usually be
/// used on a pair of a [reader](std::io::BufRead) and an [encoder](PluginEncoder).
trait PluginRead: Send {
    /// Returns `Ok(None)` on end of stream.
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError>;

    /// Returns `Ok(None)` on end of stream.
    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError>;
}

impl<R, E> PluginRead for (R, E)
where
    R: std::io::BufRead + Send,
    E: PluginEncoder,
{
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError> {
        self.1.decode_input(&mut self.0)
    }

    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError> {
        self.1.decode_output(&mut self.0)
    }
}

/// Write [PluginInput] or [PluginOutput] to the stream.
///
/// This abstraction is really only used to make testing easier; in general this will usually be
/// used on a pair of a [writer](std::io::Write) and an [encoder](PluginEncoder).
trait PluginWrite: Send {
    fn write_input(&mut self, input: &PluginInput) -> Result<(), ShellError>;
    fn write_output(&mut self, output: &PluginOutput) -> Result<(), ShellError>;

    /// Flush any internal buffers, if applicable.
    fn flush(&mut self) -> Result<(), ShellError>;
}

impl<W, E> PluginWrite for (W, E)
where
    W: std::io::Write + Send,
    E: PluginEncoder,
{
    fn write_input(&mut self, input: &PluginInput) -> Result<(), ShellError> {
        self.1.encode_input(input, &mut self.0)
    }

    fn write_output(&mut self, output: &PluginOutput) -> Result<(), ShellError> {
        self.1.encode_output(output, &mut self.0)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        self.0.flush().map_err(|err| ShellError::IOError {
            msg: err.to_string(),
        })
    }
}

/// Iterate through values received on a `ListStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
struct PluginListStream {
    io: Arc<dyn StreamDataIo>,
    id: StreamId,
}

impl Iterator for PluginListStream {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        match self.io.clone().read_list(self.id) {
            Ok(value) => value,
            Err(err) => Some(Value::error(err, Span::unknown())),
        }
    }
}

impl Drop for PluginListStream {
    fn drop(&mut self) {
        // Signal that we don't need the stream anymore.
        self.io.drop_list(self.id);
    }
}

/// Create [PipelineData] for receiving a [ListStream] input.
fn make_list_stream(
    source: Arc<dyn StreamDataIo>,
    id: StreamId,
    ctrlc: Option<Arc<AtomicBool>>,
) -> PipelineData {
    PipelineData::ListStream(
        ListStream::from_stream(PluginListStream { io: source, id }.fuse(), ctrlc),
        None,
    )
}

/// Iterate through byte chunks received on the `stdout` stream of an `ListStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
struct PluginExternalStdoutStream {
    io: Arc<dyn StreamDataIo>,
    id: StreamId,
}

impl Iterator for PluginExternalStdoutStream {
    type Item = Result<Vec<u8>, ShellError>;

    fn next(&mut self) -> Option<Result<Vec<u8>, ShellError>> {
        self.io.clone().read_external_stdout(self.id).transpose()
    }
}

impl Drop for PluginExternalStdoutStream {
    fn drop(&mut self) {
        // Signal that we don't need the stream anymore.
        self.io.drop_external_stdout(self.id);
    }
}

/// Iterate through byte chunks received on the `stderr` stream of an `ListStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
struct PluginExternalStderrStream {
    io: Arc<dyn StreamDataIo>,
    id: StreamId,
}

impl Iterator for PluginExternalStderrStream {
    type Item = Result<Vec<u8>, ShellError>;

    fn next(&mut self) -> Option<Result<Vec<u8>, ShellError>> {
        self.io.clone().read_external_stderr(self.id).transpose()
    }
}

impl Drop for PluginExternalStderrStream {
    fn drop(&mut self) {
        // Signal that we don't need the stream anymore.
        self.io.drop_external_stderr(self.id);
    }
}

/// Iterate through values received on the `exit_code` stream of an `ListStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
struct PluginExternalExitCodeStream {
    io: Arc<dyn StreamDataIo>,
    id: StreamId,
}

impl Iterator for PluginExternalExitCodeStream {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        match self.io.clone().read_external_exit_code(self.id) {
            Ok(value) => value,
            Err(err) => Some(Value::error(err, Span::unknown())),
        }
    }
}

impl Drop for PluginExternalExitCodeStream {
    fn drop(&mut self) {
        // Signal that we don't need the stream anymore.
        self.io.clone().drop_external_exit_code(self.id);
    }
}

/// Create [PipelineData] for receiving an [ExternalStream] input.
fn make_external_stream(
    source: Arc<dyn StreamDataIo>,
    id: StreamId,
    info: &ExternalStreamInfo,
    ctrlc: Option<Arc<AtomicBool>>,
) -> PipelineData {
    PipelineData::ExternalStream {
        stdout: info.stdout.as_ref().map(|stdout_info| {
            let stream = PluginExternalStdoutStream {
                io: source.clone(),
                id,
            }
            .fuse();
            let mut raw = RawStream::new(
                Box::new(stream),
                ctrlc.clone(),
                info.span,
                stdout_info.known_size,
            );
            raw.is_binary = stdout_info.is_binary;
            raw
        }),
        stderr: info.stderr.as_ref().map(|stderr_info| {
            let stream = PluginExternalStderrStream {
                io: source.clone(),
                id,
            }
            .fuse();
            let mut raw = RawStream::new(
                Box::new(stream),
                ctrlc.clone(),
                info.span,
                stderr_info.known_size,
            );
            raw.is_binary = stderr_info.is_binary;
            raw
        }),
        exit_code: info.has_exit_code.then(|| {
            ListStream::from_stream(
                PluginExternalExitCodeStream {
                    io: source.clone(),
                    id,
                }
                .fuse(),
                ctrlc.clone(),
            )
        }),
        span: info.span,
        metadata: None,
        trim_end_newline: info.trim_end_newline,
    }
}

/// Return the next available id from an accumulator, returning an error on overflow
#[track_caller]
fn next_id_from(accumulator: &AtomicUsize) -> Result<usize, ShellError> {
    // This is implemented by load, add, then CAS, to ensure uniqueness even if another thread
    // tries to get an id at the same time.
    //
    // It's totally safe to use Relaxed ordering here, as there aren't other memory operations
    // that depend on this value having been set for safety
    //
    // We're only not using `fetch_add` so that we can check for overflow, as wrapping with the
    // identifier would lead to a serious bug - however unlikely that is.
    loop {
        let current = accumulator.load(Relaxed);
        if let Some(new) = current.checked_add(1) {
            if accumulator
                .compare_exchange_weak(current, new, Relaxed, Relaxed)
                .is_ok()
            {
                // Successfully got the new id - guaranteed no other thread got the same one.
                return Ok(current);
            }
        } else {
            return Err(ShellError::NushellFailedHelp {
                msg: "an accumulator for identifiers overflowed".into(),
                help: format!("see {}", std::panic::Location::caller()),
            });
        }
    }
}
