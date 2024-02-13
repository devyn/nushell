//! Implements the stream multiplexing interface for both the plugin side and the engine side.

use std::{sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    Arc, Mutex,
}, io::Write};

use nu_protocol::{ListStream, PipelineData, RawStream, ShellError, Span, Value};

use crate::{
    plugin::PluginEncoder,
    protocol::{
        ExternalStreamInfo, ListStreamInfo, PluginInput, PluginOutput, RawStreamInfo, StreamId,
    },
};

//mod buffers;
//mod interrupt;
mod stream;

//mod stream_data_io;
//pub(crate) use stream_data_io::StreamDataIo;

mod engine;
pub use engine::EngineInterface;

mod plugin;
pub(crate) use plugin::PluginInterface;

#[cfg(test)]
mod test_util;

/// Read [PluginInput] or [PluginOutput] from the stream.
///
/// The read should be atomic, without interference from other threads.
pub(crate) trait PluginRead: Send {
    /// Returns `Ok(None)` on end of stream.
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError>;

    /// Returns `Ok(None)` on end of stream.
    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError>;
}

impl<E> PluginRead for (std::io::Stdin, E) where E: PluginEncoder {
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError> {
        let mut lock = self.0.lock();
        self.1.decode_input(&mut lock)
    }

    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError> {
        let mut lock = self.0.lock();
        self.1.decode_output(&mut lock)
    }
}

impl<R, E> PluginRead for (Mutex<R>, E)
where
    R: std::io::BufRead + Send,
    E: PluginEncoder,
{
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError> {
        let mut lock = self.0.lock().map_err(|_| ShellError::NushellFailed {
            msg: "reader mutex poisoned".into(),
        })?;
        self.1.decode_input(&mut *lock)
    }

    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError> {
        let mut lock = self.0.lock().map_err(|_| ShellError::NushellFailed {
            msg: "reader mutex poisoned".into(),
        })?;
        self.1.decode_output(&mut *lock)
    }
}

/// Write [PluginInput] or [PluginOutput] to the stream.
///
/// The write should be atomic, without interference from other threads.
pub(crate) trait PluginWrite: Send {
    fn write_input(&mut self, input: &PluginInput) -> Result<(), ShellError>;
    fn write_output(&mut self, output: &PluginOutput) -> Result<(), ShellError>;

    /// Flush any internal buffers, if applicable.
    fn flush(&mut self) -> Result<(), ShellError>;
}

impl<E> PluginWrite for (std::io::Stdout, E) where E: PluginEncoder {
    fn write_input(&mut self, input: &PluginInput) -> Result<(), ShellError> {
        let mut lock = self.0.lock();
        self.1.encode_input(input, &mut lock)
    }

    fn write_output(&mut self, output: &PluginOutput) -> Result<(), ShellError> {
        let mut lock = self.0.lock();
        self.1.encode_output(output, &mut lock)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        self.0.lock().flush().map_err(|err| ShellError::IOError {
            msg: err.to_string(),
        })
    }
}

impl<W, E> PluginWrite for (Mutex<W>, E)
where
    W: std::io::Write + Send,
    E: PluginEncoder,
{
    fn write_input(&mut self, input: &PluginInput) -> Result<(), ShellError> {
        let mut lock = self.0.lock().map_err(|_| ShellError::NushellFailed {
            msg: "writer mutex poisoned".into(),
        })?;
        self.1.encode_input(input, &mut *lock)
    }

    fn write_output(&mut self, output: &PluginOutput) -> Result<(), ShellError> {
        let mut lock = self.0.lock().map_err(|_| ShellError::NushellFailed {
            msg: "writer mutex poisoned".into(),
        })?;
        self.1.encode_output(output, &mut *lock)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        let mut lock = self.0.lock().map_err(|_| ShellError::NushellFailed {
            msg: "writer mutex poisoned".into(),
        })?;
        lock.flush().map_err(|err| ShellError::IOError {
            msg: err.to_string(),
        })
    }
}

/// Iterate through values received on a `ListStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
// struct PluginListStream {
//     io: Arc<dyn StreamDataIo>,
//     id: StreamId,
// }

// impl Iterator for PluginListStream {
//     type Item = Value;

//     fn next(&mut self) -> Option<Value> {
//         match self.io.clone().read_list(self.id) {
//             Ok(value) => value,
//             Err(err) => Some(Value::error(err, Span::unknown())),
//         }
//     }
// }

// impl Drop for PluginListStream {
//     fn drop(&mut self) {
//         // Signal that we don't need the stream anymore.
//         if let Err(err) = self.io.drop_list(self.id) {
//             log::warn!("Error while dropping PluginListStream: {err}");
//         }
//     }
// }

/// Create [`PipelineData`] for receiving a [`ListStream`] input.
// fn make_pipe_list_stream(
//     source: Arc<dyn StreamDataIo>,
//     info: &ListStreamInfo,
//     ctrlc: Option<Arc<AtomicBool>>,
// ) -> PipelineData {
//     PipelineData::ListStream(make_list_stream(source, info, ctrlc), None)
// }

/// Create a [`ListStream`] for receiving input from `source`.
// fn make_list_stream(
//     source: Arc<dyn StreamDataIo>,
//     info: &ListStreamInfo,
//     ctrlc: Option<Arc<AtomicBool>>,
// ) -> ListStream {
//     ListStream::from_stream(
//         PluginListStream {
//             io: source,
//             id: info.id,
//         }
//         .fuse(),
//         ctrlc,
//     )
// }

/// Iterate through byte chunks received on a `RawStream` input.
///
/// Non-fused iterator: should generally call .fuse() when using it, to ensure messages aren't
/// attempted to be read after end-of-input.
// struct PluginRawStream {
//     io: Arc<dyn StreamDataIo>,
//     id: StreamId,
// }

// impl Iterator for PluginRawStream {
//     type Item = Result<Vec<u8>, ShellError>;

//     fn next(&mut self) -> Option<Result<Vec<u8>, ShellError>> {
//         self.io.clone().read_raw(self.id).transpose()
//     }
// }

// impl Drop for PluginRawStream {
//     fn drop(&mut self) {
//         // Signal that we don't need the stream anymore.
//         if let Err(err) = self.io.drop_raw(self.id) {
//             log::warn!("Error while dropping PluginRawStream: {err}");
//         }
//     }
// }

/// Create a [`RawStream`] for receiving raw input from `source`.
// fn make_raw_stream(
//     source: Arc<dyn StreamDataIo>,
//     info: &RawStreamInfo,
//     span: Span,
//     ctrlc: Option<Arc<AtomicBool>>,
// ) -> RawStream {
//     let stream = PluginRawStream {
//         io: source.clone(),
//         id: info.id,
//     }
//     .fuse();
//     let mut raw = RawStream::new(Box::new(stream), ctrlc.clone(), span, info.known_size);
//     raw.is_binary = info.is_binary;
//     raw
// }

/// Create [PipelineData] for receiving an [ExternalStream] input.
// fn make_pipe_external_stream(
//     source: Arc<dyn StreamDataIo>,
//     info: &ExternalStreamInfo,
//     ctrlc: Option<Arc<AtomicBool>>,
// ) -> PipelineData {
//     PipelineData::ExternalStream {
//         stdout: info.stdout.as_ref().map(|stdout_info| {
//             make_raw_stream(source.clone(), stdout_info, info.span, ctrlc.clone())
//         }),
//         stderr: info.stderr.as_ref().map(|stderr_info| {
//             make_raw_stream(source.clone(), stderr_info, info.span, ctrlc.clone())
//         }),
//         exit_code: info
//             .exit_code
//             .as_ref()
//             .map(|exit_code_info| make_list_stream(source.clone(), exit_code_info, ctrlc.clone())),
//         span: info.span,
//         metadata: None,
//         trim_end_newline: info.trim_end_newline,
//     }
// }

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
