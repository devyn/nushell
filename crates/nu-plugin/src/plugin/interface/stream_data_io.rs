use std::collections::VecDeque;

use nu_protocol::{ShellError, Value};

// This trait must be object safe.
pub(crate) trait StreamDataIo: Send + Sync {
    fn read_list(&self) -> Result<Option<Value>, ShellError>;
    fn read_external_stdout(&self) -> Result<Option<Vec<u8>>, ShellError>;
    fn read_external_stderr(&self) -> Result<Option<Vec<u8>>, ShellError>;
    fn read_external_exit_code(&self) -> Result<Option<Value>, ShellError>;

    fn drop_list(&self);
    fn drop_external_stdout(&self);
    fn drop_external_stderr(&self);
    fn drop_external_exit_code(&self);

    fn write_list(&self, value: Option<Value>) -> Result<(), ShellError>;
    fn write_external_stdout(&self, bytes: Option<Result<Vec<u8>, ShellError>>)
        -> Result<(), ShellError>;
    fn write_external_stderr(&self, bytes: Option<Result<Vec<u8>, ShellError>>)
        -> Result<(), ShellError>;
    fn write_external_exit_code(&self, code: Option<Value>) -> Result<(), ShellError>;
}

macro_rules! impl_stream_data_io {
    (
        $type:ident,
        $read_type:ident ($decode_method:ident),
        $write_type:ident ($encode_method:ident)
    ) => {
        impl<R, W, E> StreamDataIo for $type<R, W, E>
        where
            R: BufRead + Send,
            W: Write + Send,
            E: PluginEncoder,
        {
            fn read_list(&self) -> Result<Option<Value>, ShellError> {
                let mut read = self.read.lock().expect("read mutex poisoned");
                // Read from the buffer first
                if let Some(value) = read.1.list.pop_front() {
                    Ok(value)
                } else {
                    // If we are expecting list stream data, there aren't any other simultaneous
                    // streams, so we don't need to loop. Just try to read and reject it
                    // otherwise
                    match self.encoder.$decode_method(&mut read.0)? {
                        Some($read_type::StreamData(StreamData::List(value))) => Ok(value),
                        _ => Err(ShellError::PluginFailedToDecode {
                            msg: "Expected list stream data".into(),
                        }),
                    }
                }
            }

            fn read_external_stdout(&self) -> Result<Option<Vec<u8>>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(bytes) = read.1.external_stdout.pop_front() {
                        return Ok(bytes.transpose()?);
                    } else {
                        // Skip messages from other streams until we get what we want
                        match self.encoder.$decode_method(&mut read.0)? {
                            Some($read_type::StreamData(StreamData::ExternalStdout(bytes))) =>
                                return Ok(bytes.transpose()?),
                            Some($read_type::StreamData(other)) => read.1.skip(other)?,
                            _ => return Err(ShellError::PluginFailedToDecode {
                                msg: "Expected external stream data".into()
                            }),
                        }
                    }
                }
            }

            fn read_external_stderr(&self) -> Result<Option<Vec<u8>>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(bytes) = read.1.external_stderr.pop_front() {
                        return Ok(bytes.transpose()?);
                    } else {
                        // Skip messages from other streams until we get what we want
                        match self.encoder.$decode_method(&mut read.0)? {
                            Some($read_type::StreamData(StreamData::ExternalStderr(bytes))) =>
                                return Ok(bytes.transpose()?),
                            Some($read_type::StreamData(other)) => read.1.skip(other)?,
                            _ => return Err(ShellError::PluginFailedToDecode {
                                msg: "Expected external stream data".into()
                            }),
                        }
                    }
                }
            }

            fn read_external_exit_code(&self) -> Result<Option<Value>, ShellError> {
                // Loop on the outside of the lock to allow other streams to make progress
                loop {
                    let mut read = self.read.lock().expect("read mutex poisoned");
                    // Read from the buffer first
                    if let Some(code) = read.1.external_exit_code.pop_front() {
                        return Ok(code);
                    } else {
                        // Skip messages from other streams until we get what we want
                        match self.encoder.$decode_method(&mut read.0)? {
                            Some($read_type::StreamData(StreamData::ExternalExitCode(code))) =>
                                return Ok(code),
                            Some($read_type::StreamData(other)) => read.1.skip(other)?,
                            _ => return Err(ShellError::PluginFailedToDecode {
                                msg: "Expected external stream data".into()
                            }),
                        }
                    }
                }
            }

            fn drop_list(&self) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if !matches!(read.1.list, StreamBuffer::NotPresent) {
                    read.1.list = StreamBuffer::Dropped;
                } else {
                    panic!("Tried to drop list stream but it's not present");
                }
            }

            fn drop_external_stdout(&self) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if !matches!(read.1.external_stdout, StreamBuffer::NotPresent) {
                    read.1.external_stdout = StreamBuffer::Dropped;
                } else {
                    panic!("Tried to drop external_stdout stream but it's not present");
                }
            }

            fn drop_external_stderr(&self) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if !matches!(read.1.external_stderr, StreamBuffer::NotPresent) {
                    read.1.external_stderr = StreamBuffer::Dropped;
                } else {
                    panic!("Tried to drop external_stderr stream but it's not present");
                }
            }

            fn drop_external_exit_code(&self) {
                let mut read = self.read.lock().expect("read mutex poisoned");
                if !matches!(read.1.external_exit_code, StreamBuffer::NotPresent) {
                    read.1.external_exit_code = StreamBuffer::Dropped;
                } else {
                    panic!("Tried to drop external_exit_code stream but it's not present");
                }
            }

            fn write_list(&self, value: Option<Value>) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = value.is_none();
                self.encoder.$encode_method(
                    &$write_type::StreamData(StreamData::List(value)), &mut *write)?;
                // Try to flush final value
                if is_final {
                    let _ = write.flush();
                }
                Ok(())
            }

            fn write_external_stdout(&self, bytes: Option<Result<Vec<u8>, ShellError>>)
                -> Result<(), ShellError>
            {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = bytes.is_none();
                self.encoder.$encode_method(
                    &$write_type::StreamData(StreamData::ExternalStdout(bytes)), &mut *write)?;
                // Try to flush final value
                if is_final {
                    let _ = write.flush();
                }
                Ok(())
            }

            fn write_external_stderr(&self, bytes: Option<Result<Vec<u8>, ShellError>>)
                -> Result<(), ShellError>
            {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = bytes.is_none();
                self.encoder.$encode_method(
                    &$write_type::StreamData(StreamData::ExternalStderr(bytes)), &mut *write)?;
                // Try to flush final value
                if is_final {
                    let _ = write.flush();
                }
                Ok(())
            }

            fn write_external_exit_code(&self, code: Option<Value>) -> Result<(), ShellError> {
                let mut write = self.write.lock().expect("write mutex poisoned");
                let is_final = code.is_none();
                self.encoder.$encode_method(
                    &$write_type::StreamData(StreamData::ExternalExitCode(code)), &mut *write)?;
                // Try to flush final value
                if is_final {
                    let _ = write.flush();
                }
                Ok(())
            }
        }
    }
}

pub(crate) use impl_stream_data_io;

use crate::protocol::StreamData;

/// Buffers for stream messages that temporarily can't be handled
#[derive(Default)]
pub(crate) struct StreamBuffers {
    pub list: StreamBuffer<Option<Value>>,
    pub external_stdout: StreamBuffer<Option<Result<Vec<u8>, ShellError>>>,
    pub external_stderr: StreamBuffer<Option<Result<Vec<u8>, ShellError>>>,
    pub external_exit_code: StreamBuffer<Option<Value>>,
}

#[derive(Default)]
pub(crate) enum StreamBuffer<T> {
    #[default]
    NotPresent,
    Dropped,
    Present(VecDeque<T>)
}

impl<T> StreamBuffer<T> {
    pub fn present_if(condition: bool) -> StreamBuffer<T> {
        if condition {
            StreamBuffer::Present(VecDeque::new())
        } else {
            StreamBuffer::NotPresent
        }
    }

    pub fn push_back(&mut self, value: T) -> Result<(), ShellError> {
        match self {
            StreamBuffer::NotPresent => Err(ShellError::PluginFailedToDecode {
                msg: "Tried to read into a stream that is not present".into(),
            }),
            StreamBuffer::Dropped => Ok(()), // just silently drop the message
            StreamBuffer::Present(ref mut buf) => {
                buf.push_back(value);
                Ok(())
            }
        }
    }

    pub fn pop_front(&mut self) -> Option<T> {
        match self {
            StreamBuffer::Present(ref mut buf) => buf.pop_back(),

            // Panics are used here because this would be a problem with code, not with the
            // interface.
            StreamBuffer::NotPresent =>
                panic!("Tried to read from a stream that is not present"),
            StreamBuffer::Dropped =>
                panic!("Tried to read from a stream that is already dropped"),
        }
    }

    pub fn is_dropped(&self) -> bool {
        matches!(self, StreamBuffer::Dropped)
    }
}

impl StreamBuffers {
    pub fn new_list() -> StreamBuffers {
        StreamBuffers {
            list: StreamBuffer::Present(VecDeque::new()),
            external_stdout: StreamBuffer::NotPresent,
            external_stderr: StreamBuffer::NotPresent,
            external_exit_code: StreamBuffer::NotPresent,
        }
    }

    pub fn new_external(
        has_stdout: bool,
        has_stderr: bool,
        has_exit_code: bool
    ) -> StreamBuffers {
        StreamBuffers {
            list: StreamBuffer::NotPresent,
            external_stdout: StreamBuffer::present_if(has_stdout),
            external_stderr: StreamBuffer::present_if(has_stderr),
            external_exit_code: StreamBuffer::present_if(has_exit_code),
        }
    }

    /// If a message from another stream can't be handled, it can be pushed onto the buffers
    pub fn skip(&mut self, data: StreamData) -> Result<(), ShellError> {
        match data {
            StreamData::List(val) => self.list.push_back(val),
            StreamData::ExternalStdout(bytes) => self.external_stdout.push_back(bytes),
            StreamData::ExternalStderr(bytes) => self.external_stderr.push_back(bytes),
            StreamData::ExternalExitCode(code) => self.external_exit_code.push_back(code),
        }
    }
}

