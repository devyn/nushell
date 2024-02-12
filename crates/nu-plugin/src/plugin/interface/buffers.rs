use std::collections::VecDeque;

use nu_protocol::{ShellError, Value};

use crate::{
    protocol::{PipelineDataHeader, StreamId},
    StreamData,
};

#[cfg(test)]
mod tests;

/// Buffers for stream messages that temporarily can't be handled
#[derive(Debug, Default)]
pub(crate) struct StreamBuffers {
    streams: Vec<(StreamId, Box<PerStreamBuffers>)>,
}

impl StreamBuffers {
    /// Get the buffers for a stream by id. Returns an error if the stream is not present.
    pub fn get(&mut self, id: StreamId) -> Result<&mut PerStreamBuffers, ShellError> {
        self.streams
            .iter_mut()
            .find(|(found_id, _)| *found_id == id)
            .map(|(_, bufs)| &mut **bufs)
            .ok_or_else(|| ShellError::PluginFailedToDecode {
                msg: format!("Tried to write to a non-existent stream: {id}"),
            })
    }

    /// Insert a new stream by id. Returns an error if the stream is already present.
    pub fn insert(&mut self, id: StreamId, buffers: PerStreamBuffers) -> Result<(), ShellError> {
        // Ensure the stream doesn't already exist
        if self.streams.iter().any(|(found_id, _)| *found_id == id) {
            Err(ShellError::PluginFailedToDecode {
                msg: format!("Tried to initialize already existing stream: {id}"),
            })
        } else {
            self.streams.push((id, Box::new(buffers)));
            Ok(())
        }
    }

    /// Remove any streams that were fully consumed.
    pub fn cleanup(&mut self) {
        self.streams.retain(|(id, bufs)| {
            if bufs.is_fully_consumed() {
                log::trace!("Cleaning up stream id={id}");
                false
            } else {
                true
            }
        });
    }

    /// Create buffers for the given stream header.
    ///
    /// Returns an error if the specified stream id already existed.
    pub fn init_stream(&mut self, header: &PipelineDataHeader) -> Result<(), ShellError> {
        match header {
            PipelineDataHeader::ListStream(id) => {
                log::trace!("New list stream id={id}");
                self.insert(*id, PerStreamBuffers::new_list())
            }

            PipelineDataHeader::ExternalStream(id, info) => {
                log::trace!("New external stream id={id}");
                self.insert(
                    *id,
                    PerStreamBuffers::new_external(
                        info.stdout.is_some(),
                        info.stderr.is_some(),
                        info.has_exit_code,
                    ),
                )
            }

            // Don't have to do anything for these
            PipelineDataHeader::Empty
            | PipelineDataHeader::Value(_)
            | PipelineDataHeader::PluginData(_) => Ok(()),
        }
    }

    /// Temporarily store [StreamData] for a stream. Use this if a message was received that belongs
    /// to a stream other than the one actively being read.
    pub fn skip(&mut self, id: StreamId, data: StreamData) -> Result<(), ShellError> {
        self.get(id)?.push_back(data)
    }
}

/// Buffers for stream messages for each stream
#[derive(Debug)]
pub(crate) enum PerStreamBuffers {
    List(StreamBuffer<Value>),
    External {
        stdout: StreamBuffer<Result<Vec<u8>, ShellError>>,
        stderr: StreamBuffer<Result<Vec<u8>, ShellError>>,
        exit_code: StreamBuffer<Value>,
    },
}

impl PerStreamBuffers {
    /// Create a new [PerStreamBuffers] with an empty buffer for a `ListStream`.
    ///
    /// Other stream messages will be rejected with an error.
    pub const fn new_list() -> PerStreamBuffers {
        PerStreamBuffers::List(StreamBuffer::Present {
            queue: VecDeque::new(),
            ended: false,
        })
    }

    /// Create a new [PerStreamBuffers] with empty buffers for an `ExternalStream`.
    ///
    /// The buffers will be `Present` according to the values of the parameters. Any stream messages
    /// that do not belong to streams specified as present here will be rejected with an error.
    pub const fn new_external(
        has_stdout: bool,
        has_stderr: bool,
        has_exit_code: bool,
    ) -> PerStreamBuffers {
        PerStreamBuffers::External {
            stdout: StreamBuffer::present_if(has_stdout),
            stderr: StreamBuffer::present_if(has_stderr),
            exit_code: StreamBuffer::present_if(has_exit_code),
        }
    }

    /// Push a stream message onto the correct buffer. Returns an error if the message is not
    /// accepted.
    pub fn push_back(&mut self, message: StreamData) -> Result<(), ShellError> {
        match self {
            PerStreamBuffers::List(buf) => match message {
                StreamData::List(value) => buf.push_back(value),
                StreamData::ExternalStdout(..)
                | StreamData::ExternalStderr(..)
                | StreamData::ExternalExitCode(..) => Err(ShellError::PluginFailedToDecode {
                    msg: "Tried to send an external stream's data to a list stream".into(),
                }),
            },
            PerStreamBuffers::External {
                stdout,
                stderr,
                exit_code,
            } => match message {
                StreamData::List(..) => Err(ShellError::PluginFailedToDecode {
                    msg: "Tried to send a list stream's data to an external stream".into(),
                }),
                StreamData::ExternalStdout(value) => stdout.push_back(value),
                StreamData::ExternalStderr(value) => stderr.push_back(value),
                StreamData::ExternalExitCode(value) => exit_code.push_back(value),
            },
        }
    }

    /// Pop a list value. Error if this is not a list stream.
    pub fn pop_list(&mut self) -> Result<Option<Option<Value>>, ShellError> {
        match self {
            PerStreamBuffers::List(buf) => buf.pop_front(),
            _ => Err(ShellError::NushellFailed {
                msg: "tried to read list message from non-list stream".into(),
            }),
        }
    }

    /// Pop an external stdout value. Error if this is not an external stream.
    pub fn pop_external_stdout(
        &mut self,
    ) -> Result<Option<Option<Result<Vec<u8>, ShellError>>>, ShellError> {
        match self {
            PerStreamBuffers::External { stdout, .. } => stdout.pop_front(),
            _ => Err(ShellError::NushellFailed {
                msg: "tried to read external message from non-external stream".into(),
            }),
        }
    }

    /// Pop an external stderr value. Error if this is not an external stream.
    pub fn pop_external_stderr(
        &mut self,
    ) -> Result<Option<Option<Result<Vec<u8>, ShellError>>>, ShellError> {
        match self {
            PerStreamBuffers::External { stderr, .. } => stderr.pop_front(),
            _ => Err(ShellError::NushellFailed {
                msg: "tried to read external message from non-external stream".into(),
            }),
        }
    }

    /// Pop an external exit code value. Error if this is not an external stream.
    pub fn pop_external_exit_code(&mut self) -> Result<Option<Option<Value>>, ShellError> {
        match self {
            PerStreamBuffers::External { exit_code, .. } => exit_code.pop_front(),
            _ => Err(ShellError::NushellFailed {
                msg: "tried to read external message from non-external stream".into(),
            }),
        }
    }

    /// End the list stream.
    pub fn end_list(&mut self) {
        match self {
            PerStreamBuffers::List(buf) => buf.set_ended(),
            _ => (),
        }
    }

    /// End the external stdout stream.
    pub fn end_external_stdout(&mut self) {
        match self {
            PerStreamBuffers::External { stdout, .. } => stdout.set_ended(),
            _ => (),
        }
    }

    /// End the external stderr stream.
    pub fn end_external_stderr(&mut self) {
        match self {
            PerStreamBuffers::External { stderr, .. } => stderr.set_ended(),
            _ => (),
        }
    }

    /// End the external exit code stream.
    pub fn end_external_exit_code(&mut self) {
        match self {
            PerStreamBuffers::External { exit_code, .. } => exit_code.set_ended(),
            _ => (),
        }
    }

    /// Drop the list stream.
    pub fn drop_list(&mut self) {
        match self {
            PerStreamBuffers::List(buf) => buf.set_dropped(),
            _ => (),
        }
    }

    /// Drop the external stdout stream.
    pub fn drop_external_stdout(&mut self) {
        match self {
            PerStreamBuffers::External { stdout, .. } => stdout.set_dropped(),
            _ => (),
        }
    }

    /// Drop the external stderr stream.
    pub fn drop_external_stderr(&mut self) {
        match self {
            PerStreamBuffers::External { stderr, .. } => stderr.set_dropped(),
            _ => (),
        }
    }

    /// Drop the external exit code stream.
    pub fn drop_external_exit_code(&mut self) {
        match self {
            PerStreamBuffers::External { exit_code, .. } => exit_code.set_dropped(),
            _ => (),
        }
    }

    /// True if all of the streams have been fully consumed.
    pub fn is_fully_consumed(&self) -> bool {
        match self {
            PerStreamBuffers::List(buf) => buf.is_fully_consumed(),
            PerStreamBuffers::External {
                stdout,
                stderr,
                exit_code,
            } => {
                stdout.is_fully_consumed()
                    && stderr.is_fully_consumed()
                    && exit_code.is_fully_consumed()
            }
        }
    }
}

/// A buffer for stream messages that need to be stored temporarily to allow a different stream
/// to be consumed.
///
/// The buffer is a FIFO queue.
#[derive(Debug, Default)]
pub(crate) enum StreamBuffer<T> {
    /// The default state: this stream was not specified for use, so there is no buffer available
    /// and reading a message directed for this stream will cause an error.
    #[default]
    NotPresent,
    /// This stream was specified for use, but the reader was dropped, so no further messages are
    /// desired. Any messages read that were directed for this stream will just be silently
    /// discarded, but the end of the stream will still be tracked.
    Dropped { ended: bool },
    /// This stream was specified for use, and there is still a living reader that expects messages
    /// from it. We store messages temporarily to allow another stream to proceed out-of-order.
    Present {
        queue: VecDeque<T>,
        ended: bool, // no more messages expected
    },
}

impl<T> StreamBuffer<T> {
    /// Returns a [StreamBuffer::Present] with a new empty buffer.
    pub const fn present() -> StreamBuffer<T> {
        StreamBuffer::Present {
            queue: VecDeque::new(),
            ended: false,
        }
    }

    /// Returns a [StreamBuffer::Present] with a new empty buffer if the `condition` is true, or
    /// else returns [StreamBuffer::NotPresent].
    pub const fn present_if(condition: bool) -> StreamBuffer<T> {
        if condition {
            StreamBuffer::present()
        } else {
            StreamBuffer::NotPresent
        }
    }

    /// Push a message onto the back of the buffer.
    ///
    /// Returns an error if this buffer is `NotPresent`. Discards the message if it is `Dropped`.
    pub fn push_back(&mut self, value: Option<T>) -> Result<(), ShellError> {
        match self {
            StreamBuffer::NotPresent => Err(ShellError::PluginFailedToDecode {
                msg: "Tried to write into a stream that is not present".into(),
            }),
            StreamBuffer::Dropped { ended } => {
                // Still track end of stream state
                if value.is_none() {
                    *ended = true;
                }
                // Just silently drop the message
                Ok(())
            }
            StreamBuffer::Present { queue, ended } => {
                if let Some(value) = value {
                    if !*ended {
                        queue.push_back(value);
                        Ok(())
                    } else {
                        Err(ShellError::PluginFailedToDecode {
                            msg: "Tried to write into a stream after it was closed".into(),
                        })
                    }
                } else {
                    *ended = true;
                    Ok(())
                }
            }
        }
    }

    /// Try to pop a message from the front of the buffer.
    ///
    /// Returns `Ok(None)` if there are no messages waiting in the buffer, `Ok(Some(None))` at end
    /// of stream, or an error if this buffer is either `NotPresent`, or `Dropped`.
    pub fn pop_front(&mut self) -> Result<Option<Option<T>>, ShellError> {
        match self {
            StreamBuffer::Present { queue, ended } => Ok(queue
                .pop_front()
                .map(Some)
                .or_else(|| if *ended { Some(None) } else { None })),

            StreamBuffer::NotPresent => Err(ShellError::PluginFailedToDecode {
                msg: "Tried to read from a stream that is not present".into(),
            }),
            StreamBuffer::Dropped { .. } => Err(ShellError::PluginFailedToDecode {
                msg: "Tried to read from a stream that is already dropped".into(),
            }),
        }
    }

    /// True if the buffer is `Present`.
    #[allow(dead_code)]
    pub fn is_present(&self) -> bool {
        matches!(self, StreamBuffer::Present { .. })
    }

    /// True if the buffer is present/dropped, has ended, and contains no messages,
    /// or is not present.
    pub fn is_fully_consumed(&self) -> bool {
        match self {
            StreamBuffer::Present { queue, ended } => *ended && queue.is_empty(),
            StreamBuffer::Dropped { ended } => *ended,
            _ => true,
        }
    }

    /// True if the buffer is `NotPresent`.
    #[allow(dead_code)]
    pub fn is_not_present(&self) -> bool {
        matches!(self, StreamBuffer::NotPresent)
    }

    /// True if the buffer is `Dropped`.
    pub fn is_dropped(&self) -> bool {
        matches!(self, StreamBuffer::Dropped { .. })
    }

    /// Set the stream to ended
    pub fn set_ended(&mut self) {
        match self {
            StreamBuffer::NotPresent => (), // probably not worth throwing an error
            StreamBuffer::Dropped { ended, .. } => {
                *ended = true;
            }
            StreamBuffer::Present { ended, .. } => {
                *ended = true;
            }
        }
    }

    /// Set the stream to dropped
    pub fn set_dropped(&mut self) {
        match self {
            StreamBuffer::NotPresent => (), // probably not worth throwing an error
            StreamBuffer::Dropped { .. } => (),
            StreamBuffer::Present { ended, .. } => {
                *self = StreamBuffer::Dropped { ended: *ended };
            }
        }
    }
}
