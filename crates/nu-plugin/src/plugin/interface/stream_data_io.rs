use std::sync::Arc;

use nu_protocol::{ListStream, PipelineData, RawStream, ShellError, Value};

use crate::protocol::{PipelineDataHeader, StreamId};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::{def_streams, gen_stream_data_tests};

/// Methods for reading and writing [crate::protocol::StreamData] contents on an interface.
///
/// The big idea here is that multiple streams can be multiplexed on a single input and output
/// stream, so we can handle multiple [PipelineData] at once.
///
/// This trait must be object safe.
pub(crate) trait StreamDataIo: Send + Sync {
    /// Get a new available [StreamId].
    fn new_stream_id(&self) -> Result<StreamId, ShellError>;

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

/// Implement [StreamDataIo] for the given type. The type is expected to have a shape similar to
/// `EngineInterfaceImpl` or `PluginInterfaceImpl`. The following struct fields must be defined:
///
/// * `read: Mutex<struct>`, where the inner struct has at least the following:
///   * field `reader: R` where `R` implements [`PluginRead`](super::PluginRead)
///   * field `stream_buffers: StreamBuffers`
///   * method `handle_out_of_order(&mut self, io: &Arc<$type>, msg: $read_type)
///       -> Result<(), ShellError>` - must store stream messages too using `skip()`
/// * `write: Mutex<W>` where `W` implements [`PluginWrite`](super::PluginWrite)
/// * `next_stream_id: AtomicUsize`
macro_rules! impl_stream_data_io {
    (
        $type:ident,
        $read_type:ident ($read_method:ident),
        $write_type:ident ($write_method:ident)
    ) => {
        impl<R, W> StreamDataIo for $type<R, W>
        where
            R: $crate::plugin::interface::PluginRead + 'static,
            W: $crate::plugin::interface::PluginWrite + 'static,
        {
            fn new_stream_id(&self) -> Result<StreamId, ShellError> {
                $crate::plugin::interface::next_id_from(&self.next_stream_id)
            }

            fn read_list(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(value) = read.stream_buffers.get(id)?.pop_list()? {
                        if value.is_none() {
                            // end of stream
                            read.stream_buffers.cleanup();
                        }
                        return Ok(value);
                    } else {
                        // Skip messages from other streams until we get what we want
                        match read.reader.$read_method()? {
                            Some($read_type::StreamData(data_id, StreamData::List(value)))
                                if data_id == id =>
                            {
                                if value.is_none() {
                                    // end of stream
                                    read.stream_buffers.get(id)?.end_list();
                                    read.stream_buffers.cleanup();
                                }
                                return Ok(value)
                            }
                            Some(other) => read.handle_out_of_order(&self, other)?,
                            None => {
                                return Err(ShellError::PluginFailedToDecode {
                                    msg: "unexpected end of input".into(),
                                });
                            }
                        }
                    }
                }
            }

            fn read_external_stdout(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(bytes) = read.stream_buffers.get(id)?.pop_external_stdout()? {
                        if bytes.is_none() {
                            // end of stream
                            read.stream_buffers.cleanup();
                        }
                        return bytes.transpose();
                    } else {
                        // Skip messages from other streams until we get what we want
                        match read.reader.$read_method()? {
                            Some($read_type::StreamData(data_id, StreamData::ExternalStdout(bytes)))
                                if data_id == id =>
                            {
                                if bytes.is_none() {
                                    // end of stream
                                    read.stream_buffers.get(id)?.end_external_stdout();
                                    read.stream_buffers.cleanup();
                                }
                                return bytes.transpose()
                            }
                            Some(other) => read.handle_out_of_order(&self, other)?,
                            None => {
                                return Err(ShellError::PluginFailedToDecode {
                                    msg: "unexpected end of input".into(),
                                });
                            }
                        }
                    }
                }
            }

            fn read_external_stderr(self: Arc<Self>, id: StreamId) -> Result<Option<Vec<u8>>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(bytes) = read.stream_buffers.get(id)?.pop_external_stderr()? {
                        if bytes.is_none() {
                            // end of stream
                            read.stream_buffers.cleanup();
                        }
                        return bytes.transpose();
                    } else {
                        // Skip messages from other streams until we get what we want
                        match read.reader.$read_method()? {
                            Some($read_type::StreamData(data_id, StreamData::ExternalStderr(bytes)))
                                if data_id == id =>
                            {
                                if bytes.is_none() {
                                    // end of stream
                                    read.stream_buffers.get(id)?.end_external_stderr();
                                    read.stream_buffers.cleanup();
                                }
                                return bytes.transpose()
                            }
                            Some(other) => read.handle_out_of_order(&self, other)?,
                            None => {
                                return Err(ShellError::PluginFailedToDecode {
                                    msg: "unexpected end of input".into(),
                                });
                            }
                        }
                    }
                }
            }

            fn read_external_exit_code(self: Arc<Self>, id: StreamId) -> Result<Option<Value>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(code) = read.stream_buffers.get(id)?.pop_external_exit_code()? {
                        if code.is_none() {
                            // end of stream
                            read.stream_buffers.cleanup();
                        }
                        return Ok(code);
                    } else {
                        // Skip messages from other streams until we get what we want
                        match read.reader.$read_method()? {
                            Some($read_type::StreamData(data_id, StreamData::ExternalExitCode(code)))
                                if data_id == id =>
                            {
                                if code.is_none() {
                                    // end of stream
                                    read.stream_buffers.get(id)?.end_external_exit_code();
                                    read.stream_buffers.cleanup();
                                }
                                return Ok(code)
                            }
                            Some(other) => read.handle_out_of_order(&self, other)?,
                            None => {
                                return Err(ShellError::PluginFailedToDecode {
                                    msg: "unexpected end of input".into(),
                                });
                            }
                        }
                    }
                }
            }

            fn drop_list(&self, id: StreamId) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if let Ok(stream) = read.stream_buffers.get(id) {
                    stream.drop_list();
                }
            }

            fn drop_external_stdout(&self, id: StreamId) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if let Ok(stream) = read.stream_buffers.get(id) {
                    stream.drop_external_stdout();
                }
            }

            fn drop_external_stderr(&self, id: StreamId) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if let Ok(stream) = read.stream_buffers.get(id) {
                    stream.drop_external_stderr();
                }
            }

            fn drop_external_exit_code(&self, id: StreamId) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if let Ok(stream) = read.stream_buffers.get(id) {
                    stream.drop_external_exit_code();
                }
            }

            fn write_list(&self, id: StreamId, value: Option<Value>) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = value.is_none();
                write.$write_method(&$write_type::StreamData(id, StreamData::List(value)))?;
                // Try to flush final value
                if is_final {
                    write.flush()?;
                }
                Ok(())
            }

            fn write_external_stdout(
                &self,
                id: StreamId,
                bytes: Option<Result<Vec<u8>, ShellError>>,
            ) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = bytes.is_none();
                write.$write_method(&$write_type::StreamData(id, StreamData::ExternalStdout(bytes)))?;
                // Try to flush final value
                if is_final {
                    write.flush()?;
                }
                Ok(())
            }

            fn write_external_stderr(
                &self,
                id: StreamId,
                bytes: Option<Result<Vec<u8>, ShellError>>,
            ) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = bytes.is_none();
                write.$write_method(&$write_type::StreamData(id, StreamData::ExternalStderr(bytes)))?;
                // Try to flush final value
                if is_final {
                    write.flush()?;
                }
                Ok(())
            }

            fn write_external_exit_code(
                &self,
                id: StreamId,
                code: Option<Value>,
            ) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = code.is_none();
                write
                    .$write_method(&$write_type::StreamData(id, StreamData::ExternalExitCode(code)))?;
                // Try to flush final value
                if is_final {
                    write.flush()?;
                }
                Ok(())
            }
        }
    };
}

pub(crate) use impl_stream_data_io;

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
