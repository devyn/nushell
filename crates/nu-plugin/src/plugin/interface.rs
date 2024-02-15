//! Implements the stream multiplexing interface for both the plugin side and the engine side.

use std::{sync::{
    atomic::{AtomicBool, Ordering::Relaxed},
    Arc, Mutex,
}, io::Write};

use nu_protocol::{ListStream, PipelineData, RawStream, ShellError, Span, Value};

use crate::{
    plugin::PluginEncoder,
    protocol::{
        ExternalStreamInfo, ListStreamInfo, PluginInput, PluginOutput, RawStreamInfo,
        StreamId, StreamMessage, PipelineDataHeader, PluginData,
    },
    sequence::Sequence,
};

//mod buffers;
//mod interrupt;
mod stream;

//mod stream_data_io;
//pub(crate) use stream_data_io::StreamDataIo;

mod engine;
pub use engine::EngineInterface;
pub(crate) use engine::EngineInterfaceManager;

mod plugin;
pub(crate) use plugin::{PluginInterface, PluginInterfaceManager};

use self::stream::{WriteStreamMessage, StreamManager, StreamWriter, StreamManagerHandle};

#[cfg(test)]
mod test_util;

/// The maximum number of list stream values to send without acknowledgement. This should be tuned
/// with consideration for memory usage.
const LIST_STREAM_HIGH_PRESSURE: i32 = 100;

/// The maximum number of raw stream buffers to send without acknowledgement. This should be tuned
/// with consideration for memory usage.
const RAW_STREAM_HIGH_PRESSURE: i32 = 50;

/// Read [PluginInput] or [PluginOutput] from the stream.
pub(crate) trait PluginRead {
    /// Returns `Ok(None)` on end of stream.
    fn read_input(&mut self) -> Result<Option<PluginInput>, ShellError>;

    /// Returns `Ok(None)` on end of stream.
    fn read_output(&mut self) -> Result<Option<PluginOutput>, ShellError>;
}

impl<R, E> PluginRead for (R, E)
where
    R: std::io::BufRead,
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
/// The write should be atomic, without interference from other threads.
pub(crate) trait PluginWrite: Send + Sync {
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

/// An interface manager handles I/O and state management for communication between a plugin and the
/// engine. See [`PluginInterfaceManager`] for communication from the engine side to a plugin, or
/// [`EngineInterfaceManager`] for communication from the plugin side to the engine.
///
/// There is typically one [`InterfaceManager`] consuming input from a background thread, and
/// managing shared state.
pub(crate) trait InterfaceManager {
    /// The corresponding interface type.
    type Interface: Interface + 'static;

    /// The input message type.
    type Input;

    /// The context type. This is fed through [`read_pipeline_data()`] to
    /// [`value_from_plugin_data()`] and can be used if there is special context required to be
    /// able to handle [`PluginData`]. Set to `()` if not needed.
    type Context;

    /// Make a new interface that communicates with this [`InterfaceManager`].
    fn get_interface(&self) -> Self::Interface;

    /// Consume an input message.
    ///
    /// When implementing, call [`consume_stream_message()`] for any encapsulated [`StreamMessage`]s
    /// received.
    fn consume(&mut self, input: Self::Input) -> Result<(), ShellError>;

    /// Convert [`PluginData`] to a [`Value`]. This should support plugin custom values as
    /// appropriate to the interface.
    fn value_from_plugin_data(
        &self,
        data: PluginData,
        context: &Self::Context,
    ) -> Result<Value, ShellError>;

    /// Get the interrupt signal from the given context.
    fn ctrlc(&self, context: &Self::Context) -> Option<Arc<AtomicBool>>;

    /// Get the [`StreamManager`] for handling operations related to stream messages.
    fn stream_manager(&self) -> &StreamManager;

    /// Consume an input stream message.
    ///
    /// This method is provided for implementors to use.
    fn consume_stream_message(&mut self, message: StreamMessage) -> Result<(), ShellError> {
        self.stream_manager().handle_message(message)
    }

    /// Generate `PipelineData` for reading a stream, given a [`PipelineDataHeader`] that was
    /// received from the other side.
    ///
    /// This method is provided for implementors to use.
    fn read_pipeline_data(
        &self,
        header: PipelineDataHeader,
        context: &Self::Context,
    ) -> Result<PipelineData, ShellError> {
        match header {
            PipelineDataHeader::Empty => Ok(PipelineData::Empty),
            PipelineDataHeader::Value(value) => Ok(PipelineData::Value(value, None)),
            PipelineDataHeader::PluginData(data) => {
                let value = self.value_from_plugin_data(data, context)?;
                Ok(PipelineData::Value(value, None))
            }
            PipelineDataHeader::ListStream(info) => {
                let handle = self.stream_manager().get_handle();
                let reader = handle.read_stream(info.id, self.get_interface())?;
                Ok(PipelineData::ListStream(
                    ListStream::from_stream(reader, self.ctrlc(context)),
                    None
                ))
            }
            PipelineDataHeader::ExternalStream(info) => {
                let handle = self.stream_manager().get_handle();
                let span = info.span;
                let new_raw_stream = |raw_info: RawStreamInfo| {
                    let reader = handle.read_stream(raw_info.id, self.get_interface())?;
                    let mut stream = RawStream::new(
                        Box::new(reader),
                        self.ctrlc(context),
                        span,
                        raw_info.known_size,
                    );
                    stream.is_binary = raw_info.is_binary;
                    Ok::<_, ShellError>(stream)
                };
                Ok(PipelineData::ExternalStream {
                    stdout: info.stdout.map(&new_raw_stream).transpose()?,
                    stderr: info.stderr.map(&new_raw_stream).transpose()?,
                    exit_code: info.exit_code.map(|list_info| {
                        handle.read_stream(list_info.id, self.get_interface())
                            .map(|reader| ListStream::from_stream(reader, self.ctrlc(context)))
                    }).transpose()?,
                    span: info.span,
                    metadata: None,
                    trim_end_newline: info.trim_end_newline,
                })
            }
        }
    }
}

/// An interface provides an API for communicating with a plugin or the engine and facilitates
/// stream I/O. See [`PluginInterface`] for the API from the engine side to a plugin, or
/// [`EngineInterface`] for the API from the plugin side to the engine.
///
/// There can be multiple copies of the interface managed by a single [`InterfaceManager`].
pub(crate) trait Interface: Clone + Send {
    /// The output message type, which must be capable of encapsulating a [`StreamMessage`].
    type Output: From<StreamMessage>;

    /// The context type. This is fed through [`init_write_pipeline_data()`] to
    /// [`value_to_plugin_data()`] and can be used if there is special context required to be
    /// able to handle [`PluginData`]. Set to `()` if not needed.
    type Context;

    /// Write an output message.
    fn write(&self, output: Self::Output) -> Result<(), ShellError>;

    /// Get the sequence for generating new [`StreamId`]s.
    fn stream_id_sequence(&self) -> &Sequence;

    /// Get the [`StreamManagerHandle`] for doing stream operations.
    fn stream_manager_handle(&self) -> &StreamManagerHandle;

    /// Convert a [`Value`] to [`PluginData`] if applicable. This should support plugin custom
    /// values as appropriate for the interface. Return `Ok(None)` if the `Value` does not need
    /// to be converted, or an `Err` if it is necessary but an error occurred.
    fn value_to_plugin_data(
        &self,
        value: &Value,
        context: &Self::Context,
    ) -> Result<Option<PluginData>, ShellError>;

    /// Initialize a write for [`PipelineData`]. This returns two parts: the header, which can be
    /// embedded in the particular message that references the stream, and a writer, which will
    /// write out all of the data in the pipeline when `.write()` is called.
    ///
    /// Note that not all [`PipelineData`] starts a stream. You should call `write()` anyway, as
    /// it will automatically handle this case.
    fn init_write_pipeline_data(
        &self,
        data: PipelineData,
        context: &Self::Context,
    ) -> Result<(PipelineDataHeader, PipelineDataWriter<Self>), ShellError> {
        // Allocate a stream id and a writer
        let new_stream = |high_pressure_mark: i32| {
            // Get a free stream id
            let id = self.stream_id_sequence().next()?;
            // Create the writer
            let writer = self.stream_manager_handle().write_stream(
                id,
                self.clone(),
                high_pressure_mark,
            )?;
            Ok::<_, ShellError>((id, writer))
        };
        match data {
            PipelineData::Value(value, _) => Ok((
                // CustomValues may be representable as PluginData, but the specific way they're
                // converted is interface-specific.
                match self.value_to_plugin_data(&value, context)? {
                    Some(plugin_data) => PipelineDataHeader::PluginData(plugin_data),
                    None => PipelineDataHeader::Value(value),
                },
                PipelineDataWriter::NoStream,
            )),
            PipelineData::Empty => Ok((
                PipelineDataHeader::Empty,
                PipelineDataWriter::NoStream,
            )),
            PipelineData::ListStream(stream, _) => {
                let (id, writer) = new_stream(LIST_STREAM_HIGH_PRESSURE)?;
                Ok((
                    PipelineDataHeader::ListStream(ListStreamInfo { id }),
                    PipelineDataWriter::ListStream(writer, stream)
                ))
            }
            PipelineData::ExternalStream {
                stdout,
                stderr,
                exit_code,
                span,
                metadata: _,
                trim_end_newline,
            } => {
                // Create the writers and stream ids
                let stdout_stream = stdout
                    .is_some()
                    .then(|| new_stream(RAW_STREAM_HIGH_PRESSURE))
                    .transpose()?;
                let stderr_stream = stderr
                    .is_some()
                    .then(|| new_stream(RAW_STREAM_HIGH_PRESSURE))
                    .transpose()?;
                let exit_code_stream = exit_code
                    .is_some()
                    .then(|| new_stream(LIST_STREAM_HIGH_PRESSURE))
                    .transpose()?;
                // Generate the header, with the stream ids
                let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
                    span,
                    stdout: stdout
                        .as_ref()
                        .zip(stdout_stream.as_ref())
                        .map(|(stream, (id, _))| RawStreamInfo::new(*id, stream)),
                    stderr: stderr
                        .as_ref()
                        .zip(stderr_stream.as_ref())
                        .map(|(stream, (id, _))| RawStreamInfo::new(*id, stream)),
                    exit_code: exit_code_stream.as_ref().map(|&(id, _)| ListStreamInfo { id }),
                    trim_end_newline,
                });
                // Collect the writers
                let writer = PipelineDataWriter::ExternalStream {
                    stdout: stdout_stream.map(|(_, writer)| writer).zip(stdout),
                    stderr: stderr_stream.map(|(_, writer)| writer).zip(stderr),
                    exit_code: exit_code_stream.map(|(_, writer)| writer).zip(exit_code),
                };
                Ok((header, writer))
            }
        }
    }
}

impl<T> WriteStreamMessage for T where T: Interface {
    fn write_stream_message(&mut self, msg: StreamMessage) -> Result<(), ShellError> {
        self.write(msg.into())
    }
}

/// Completes the write operation for a [`PipelineData`]. You must call
/// [`PipelineDataWriter::write()`] to write all of the data contained within the streams.
#[must_use]
pub(crate) enum PipelineDataWriter<W: WriteStreamMessage> {
    NoStream,
    ListStream(StreamWriter<W>, ListStream),
    ExternalStream {
        stdout: Option<(StreamWriter<W>, RawStream)>,
        stderr: Option<(StreamWriter<W>, RawStream)>,
        exit_code: Option<(StreamWriter<W>, ListStream)>,
    },
}

impl<W> PipelineDataWriter<W> where W: WriteStreamMessage + Send {
    /// Write all of the data in each of the streams.
    pub(crate) fn write(self) -> Result<(), ShellError> {
        match self {
            // If no stream was contained in the PipelineData, do nothing.
            PipelineDataWriter::NoStream => Ok(()),
            // Write a list stream.
            PipelineDataWriter::ListStream(mut writer, stream) => {
                writer.write_all(stream)?;
                Ok(())
            }
            // Write all three possible streams of an ExternalStream on separate threads.
            PipelineDataWriter::ExternalStream { stdout, stderr, exit_code } => {
                std::thread::scope(|scope| {
                    for thread in [
                        stdout.map(|(mut writer, stream)| {
                            scope.spawn(move || writer.write_all(raw_stream_iter(stream)))
                        }),
                        stderr.map(|(mut writer, stream)| {
                            scope.spawn(move || writer.write_all(raw_stream_iter(stream)))
                        }),
                        exit_code.map(|(mut writer, stream)| {
                            scope.spawn(move || writer.write_all(stream))
                        }),
                    ].into_iter().flatten() {
                        thread.join().map_err(|_| ShellError::NushellFailed {
                            msg: "a panic occured on an ExternalStream writer thread".into()
                        })??;
                    }
                    Ok(())
                })
            },
        }
    }
}

/// Custom iterator for [`RawStream`] that respects ctrlc, but still has binary chunks
fn raw_stream_iter(stream: RawStream) -> impl Iterator<Item=Result<Vec<u8>, ShellError>> {
    let ctrlc = stream.ctrlc;
    stream.stream.take_while(move |_| ctrlc.as_ref().map(|b| !b.load(Relaxed)).unwrap_or(true))
}
