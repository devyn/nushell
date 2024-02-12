use std::sync::{Arc, MutexGuard};

use nu_protocol::{ListStream, PipelineData, RawStream, ShellError, Value};

use crate::{protocol::{PipelineDataHeader, StreamId, StreamMessage}, StreamData};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::{def_streams, gen_stream_data_tests};

/// Methods for reading and writing [crate::protocol::StreamData] contents on an interface.
///
/// The big idea here is that multiple streams can be multiplexed on a single input and output
/// stream, so we can handle multiple [PipelineData] at once.
///
/// This trait must be object safe. Rather than implementing this trait, implement
/// [`StreamDataIoBase`] to have this trait automatically implemented.
pub(crate) trait StreamDataIo: Send + Sync {
    /// Read a value for a `ListStream`, returning `Ok(None)` at end of stream.
    ///
    /// Other streams will be transparently handled or stored for concurrent readers.
    fn read_list(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError>;

    /// Read some bytes for an `ExternalStream`'s `stdout` stream, returning `Ok(None)` at end
    /// of stream.
    ///
    /// Other streams will be transparently handled or stored for concurrent readers.
    fn read_external_stdout(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError>;

    /// Read some bytes for an `ExternalStream`'s `stderr` stream, returning `Ok(None)` at end
    /// of stream.
    ///
    /// Other streams will be transparently handled or stored for concurrent readers.
    fn read_external_stderr(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError>;

    /// Read a value for an `ExternalStream`'s `exit_code` stream, returning `Ok(None)` at end
    /// of stream.
    ///
    /// Other streams will be transparently handled or stored for concurrent readers.
    fn read_external_exit_code(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError>;

    /// Signal that no more values are desired from a `ListStream` and further messages should
    /// be ignored.
    fn drop_list(&self, id: StreamId);

    /// Signal that no more bytes are desired from an `ExternalStream`'s `stdout` and further
    /// messages should be ignored.
    fn drop_external_stdout(&self, id: StreamId);

    /// Signal that no more bytes are desired from an `ExternalStream`'s `stderr` and further
    /// messages should be ignored.
    fn drop_external_stderr(&self, id: StreamId);

    /// Signal that no more values are desired from an `ExternalStream`'s `exit_code` and further
    /// messages should be ignored.
    fn drop_external_exit_code(&self, id: StreamId);

    /// Write a value for a `ListStream`, or `None` to signal end of stream.
    fn write_list(&self, id: StreamId, value: Option<Value>) -> Result<(), ShellError>;

    /// Write some bytes for an `ExternalStream`'s `stdout` stream, or `None` to signal end of
    /// stream.
    fn write_external_stdout(
        &self,
        id: StreamId,
        bytes: Option<Result<Vec<u8>, ShellError>>,
    ) -> Result<(), ShellError>;

    /// Write some bytes for an `ExternalStream`'s `stderr` stream, or `None` to signal end of
    /// stream.
    fn write_external_stderr(
        &self,
        id: StreamId,
        bytes: Option<Result<Vec<u8>, ShellError>>,
    ) -> Result<(), ShellError>;

    /// Write a value for an `ExternalStream`'s `exit_code` stream, or `None` to signal end of
    /// stream.
    fn write_external_exit_code(&self, id: StreamId, code: Option<Value>)
        -> Result<(), ShellError>;
}

/// The base trait necessary to be implemented for [`StreamDataIo`] to work.
///
/// An implementor is generally divided into a read part and a write part, both behind separate
/// locks
pub(crate) trait StreamDataIoBase: Send + Sync {
    type ReadPart: StreamDataRead<Base=Self>;
    type WritePart: StreamDataWrite;

    /// Get exclusive access to the read part. May panic if the mutex is poisoned.
    fn lock_read(&self) -> MutexGuard<Self::ReadPart>;

    /// Get exclusive access to the write part. May panic if the mutex is poisoned.
    fn lock_write(&self) -> MutexGuard<Self::WritePart>;

    /// Get a new, locally unique [`StreamId`].
    fn new_stream_id(&self) -> Result<StreamId, ShellError>;
}

pub(crate) trait StreamDataRead {
    /// A message capable of containing `StreamMessage`.
    type Message;
    type Base: StreamDataIoBase;

    /// Read a message. Returns `Ok(None)` at end of input.
    ///
    /// This message may not be [`StreamMessage`], hence the required implementation of [`TryInto`]
    /// on the `Message` type.
    fn read(&mut self) -> Result<Option<Self::Message>, ShellError>;

    /// The [`StreamBuffers`] for storing out of order messages.
    fn stream_buffers(&mut self) -> &mut StreamBuffers;

    /// Handle an out of order [`StreamMessage`]. The default implementation adds it to the
    /// [`StreamBuffers`].
    fn handle_out_of_order(&mut self, msg: StreamMessage) -> Result<(), ShellError> {
        match msg {
            StreamMessage::Data(id, data) => self.stream_buffers().skip(id, data),
        }
    }

    /// Implements default handling of a message received outside of any specific context. For
    /// example, a stream message, or an engine call. This method may return an error for
    /// messages that require the interface to be in a specific state to be handled.
    ///
    /// The original `Arc` is passed in case it needs to be cloned - for example, to create a
    /// `dyn Iterator` for `ListStream` or `ExternalStream`.
    fn handle_message(
        &mut self,
        io: &Arc<Self::Base>,
        msg: Self::Message,
    ) -> Result<(), ShellError>;
}

pub(crate) trait StreamDataWrite {
    /// A message capable of containing `StreamMessage`.
    type Message: From<StreamMessage>;

    /// Write a message to the output stream.
    fn write(&mut self, msg: Self::Message) -> Result<(), ShellError>;

    /// Ensure all messages in the internal buffer have been written out, blocking as necessary.
    fn flush(&mut self) -> Result<(), ShellError>;
}

/// Helper for implementing the read methods.
macro_rules! read_stream_data_for {
    (
        $self:expr,
        $id:expr,
        $data_type:ident,
        pop ($pop_method:ident),
        end ($end_method:ident) $(,)?
    ) => ({
        // Loop on the outside of the lock to allow other streams to make progress
        loop {
            let mut read = $self.lock_read();
            // Read from the buffer first
            if let Some(value) = read.stream_buffers().get($id)?.$pop_method()? {
                if value.is_none() {
                    // end of stream
                    read.stream_buffers().cleanup();
                }
                break value;
            } else {
                // Skip messages from other streams until we get what we want
                match read.read()? {
                    Some(msg) => match StreamMessage::try_from(msg) {
                        Ok(StreamMessage::Data(data_id, StreamData::$data_type(value)))
                            if data_id == $id =>
                        {
                            if value.is_none() {
                                // end of stream
                                read.stream_buffers().get($id)?.$end_method();
                                read.stream_buffers().cleanup();
                            }
                            break value;
                        }
                        Ok(other) => read.handle_out_of_order(other)?,
                        Err(other) => read.handle_message(&$self, other)?,
                    }
                    None => {
                        return Err(ShellError::PluginFailedToDecode {
                            msg: "unexpected end of input".into(),
                        });
                    }
                }
            }
        }
    })
}

/// Helper for implementing the write methods
macro_rules! write_stream_data_for {
    ($self:expr, $id:expr, $data_type:ident ($value:expr)) => ({
        let mut write = $self.lock_write();
        let is_final = $value.is_none();
        write.write(StreamMessage::Data($id, StreamData::$data_type($value)).into())?;
        // Try to flush final value
        if is_final {
            write.flush()?;
        }
        Ok(())
    })
}

impl<T> StreamDataIo for T
where
    T: StreamDataIoBase,
    // It must be possible to convert the input message to StreamMessage
    StreamMessage: TryFrom<
        <T::ReadPart as StreamDataRead>::Message,
        Error=<T::ReadPart as StreamDataRead>::Message
    >,
{
    fn read_list(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError> {
        Ok(read_stream_data_for!(self, id, List, pop (pop_list), end (end_list)))
    }

    fn read_external_stdout(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError> {
        read_stream_data_for!(
            self,
            id,
            ExternalStdout,
            pop (pop_external_stdout),
            end (end_external_stdout),
        ).transpose()
    }

    fn read_external_stderr(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError> {
        read_stream_data_for!(
            self,
            id,
            ExternalStderr,
            pop (pop_external_stderr),
            end (end_external_stderr)
        ).transpose()
    }

    fn read_external_exit_code(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError> {
        Ok(read_stream_data_for!(
            self,
            id,
            ExternalExitCode,
            pop (pop_external_exit_code),
            end (end_external_exit_code)
        ))
    }

    fn drop_list(&self, id: StreamId) {
        let mut read = self.lock_read();
        if let Ok(stream) = read.stream_buffers().get(id) {
            stream.drop_list();
        }
    }

    fn drop_external_stdout(&self, id: StreamId) {
        let mut read = self.lock_read();
        if let Ok(stream) = read.stream_buffers().get(id) {
            stream.drop_external_stdout();
        }
    }

    fn drop_external_stderr(&self, id: StreamId) {
        let mut read = self.lock_read();
        if let Ok(stream) = read.stream_buffers().get(id) {
            stream.drop_external_stderr();
        }
    }

    fn drop_external_exit_code(&self, id: StreamId) {
        let mut read = self.lock_read();
        if let Ok(stream) = read.stream_buffers().get(id) {
            stream.drop_external_exit_code();
        }
    }

    fn write_list(&self, id: StreamId, value: Option<Value>) -> Result<(), ShellError> {
        write_stream_data_for!(self, id, List(value))
    }

    fn write_external_stdout(
        &self,
        id: StreamId,
        bytes: Option<Result<Vec<u8>, ShellError>>,
    ) -> Result<(), ShellError> {
        write_stream_data_for!(self, id, ExternalStdout(bytes))
    }

    fn write_external_stderr(
        &self,
        id: StreamId,
        bytes: Option<Result<Vec<u8>, ShellError>>,
    ) -> Result<(), ShellError> {
        write_stream_data_for!(self, id, ExternalStderr(bytes))
    }

    fn write_external_exit_code(
        &self,
        id: StreamId,
        code: Option<Value>,
    ) -> Result<(), ShellError> {
        write_stream_data_for!(self, id, ExternalExitCode(code))
    }
}

use super::buffers::StreamBuffers;

/// Extension trait for additional methods that can be used on any [StreamDataIo]
pub(crate) trait StreamDataIoExt: StreamDataIo {
    /// Write the contents of a [PipelineData]. This is a no-op for non-stream data.
    #[track_caller]
    fn write_pipeline_data_stream(
        &self,
        header: &PipelineDataHeader,
        data: PipelineData,
    ) -> Result<(), ShellError> {
        match (header, data) {
            (PipelineDataHeader::Empty, PipelineData::Empty) => Ok(()),
            (PipelineDataHeader::Value(_), PipelineData::Value(_, _)) => Ok(()),
            (PipelineDataHeader::PluginData(_), PipelineData::Value(_, _)) => Ok(()),
            (PipelineDataHeader::ListStream(id), PipelineData::ListStream(stream, _)) => {
                write_full_list_stream(self, *id, stream)
            }
            (
                PipelineDataHeader::ExternalStream(id, _),
                PipelineData::ExternalStream {
                    stdout,
                    stderr,
                    exit_code,
                    ..
                },
            ) => write_full_external_stream(self, *id, stdout, stderr, exit_code),
            _ => Err(ShellError::NushellFailedHelp {
                msg: format!(
                    "Attempted to send PipelineData that doesn't match the header: {:?}",
                    header
                ),
                help: format!("see {}", std::panic::Location::caller()),
            }),
        }
    }
}

impl<T: StreamDataIo + ?Sized> StreamDataIoExt for T {}

/// Write the contents of a [ListStream] to `io`.
fn write_full_list_stream(
    io: &(impl StreamDataIo + ?Sized),
    id: StreamId,
    list_stream: ListStream,
) -> Result<(), ShellError> {
    // Consume the stream and write it via StreamDataIo.
    for value in list_stream {
        io.write_list(
            id,
            Some(match value {
                Value::LazyRecord { val, .. } => val.collect()?,
                _ => value,
            }),
        )?;
    }
    // End of stream
    io.write_list(id, None)
}

/// Write the contents of a [PipelineData::ExternalStream] to `io`.
fn write_full_external_stream(
    io: &(impl StreamDataIo + ?Sized),
    id: StreamId,
    stdout: Option<RawStream>,
    stderr: Option<RawStream>,
    exit_code: Option<ListStream>,
) -> Result<(), ShellError> {
    // Consume all streams simultaneously by launching three threads
    std::thread::scope(|scope| {
        for thread in [
            stdout.map(|stdout| {
                scope.spawn(|| {
                    for bytes in stdout.stream {
                        io.write_external_stdout(id, Some(bytes))?;
                    }
                    io.write_external_stdout(id, None)
                })
            }),
            stderr.map(|stderr| {
                scope.spawn(|| {
                    for bytes in stderr.stream {
                        io.write_external_stderr(id, Some(bytes))?;
                    }
                    io.write_external_stderr(id, None)
                })
            }),
            exit_code.map(|exit_code| {
                scope.spawn(|| {
                    for value in exit_code {
                        io.write_external_exit_code(id, Some(value))?;
                    }
                    io.write_external_exit_code(id, None)
                })
            }),
        ]
        .into_iter()
        .flatten()
        {
            thread.join().expect("stream consumer thread panicked")?;
        }
        Ok(())
    })
}
