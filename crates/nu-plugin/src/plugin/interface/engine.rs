//! Interface used by the plugin to communicate with the engine.

use std::{
    io::{BufRead, Write},
    sync::{atomic::AtomicUsize, Arc, Mutex},
};

use nu_protocol::{CustomValue, PipelineData, ShellError, Value};

use crate::{
    plugin::PluginEncoder,
    protocol::{
        CallInfo, ExternalStreamInfo, PipelineDataHeader, PluginCall, PluginCallResponse,
        PluginData, PluginInput, PluginOutput, RawStreamInfo, StreamData, StreamId,
    },
};

use super::{
    buffers::StreamBuffers,
    make_external_stream, make_list_stream,
    stream_data_io::{impl_stream_data_io, StreamDataIo, StreamDataIoExt},
    PluginRead, PluginWrite,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct EngineInterfaceImpl<R, W> {
    // Always lock read and then write mutex, if using both
    // Stream inputs that can't be handled immediately can be put on the buffer
    read: Mutex<ReadPart<R>>,
    write: Mutex<W>,
    /// The next available stream id
    next_stream_id: AtomicUsize,
}

#[derive(Debug)]
struct ReadPart<R> {
    reader: R,
    stream_buffers: StreamBuffers,
}

impl<R, W> EngineInterfaceImpl<R, W> {
    pub(crate) fn new(reader: R, writer: W) -> EngineInterfaceImpl<R, W> {
        EngineInterfaceImpl {
            read: Mutex::new(ReadPart {
                reader,
                stream_buffers: StreamBuffers::default(),
            }),
            write: Mutex::new(writer),
            next_stream_id: AtomicUsize::new(0),
        }
    }
}

// Implement the stream handling methods (see StreamDataIo).
impl_stream_data_io!(
    EngineInterfaceImpl,
    PluginInput(read_input),
    PluginOutput(write_output),
    read other match {
    }
);

/// The trait indirection is so that we can hide the types with a trait object inside
/// EngineInterface. As such, this trait must remain object safe.
pub(crate) trait EngineInterfaceIo: StreamDataIo {
    fn read_call(&self) -> Result<Option<PluginCall>, ShellError>;
    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError>;

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError>;

    /// Create a valid header to send the given PipelineData.
    ///
    /// Returns the header, and the PipelineData to be sent with `write_pipeline_data_stream`
    /// if necessary.
    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<PipelineData>), ShellError>;
}

impl<R, W> EngineInterfaceIo for EngineInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn read_call(&self) -> Result<Option<PluginCall>, ShellError> {
        let mut read = self.read.lock().expect("read mutex poisoned");
        loop {
            match read.reader.read_input()? {
                Some(PluginInput::Call(call)) => {
                    // Check the call input type to set the stream buffers up
                    if let PluginCall::Run(CallInfo { ref input, .. }) = call {
                        read.stream_buffers.init_stream(input)?;
                    }
                    return Ok(Some(call));
                }
                // Skip over any remaining stream data
                Some(PluginInput::StreamData(id, data)) => read.stream_buffers.skip(id, data)?,
                // End of input
                None => return Ok(None),
            }
        }
    }

    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError> {
        let mut write = self.write.lock().expect("write mutex poisoned");

        write.write_output(&PluginOutput::CallResponse(response))?;
        write.flush()
    }

    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        match header {
            PipelineDataHeader::Empty => Ok(PipelineData::Empty),
            PipelineDataHeader::Value(value) => Ok(PipelineData::Value(value, None)),
            PipelineDataHeader::PluginData(plugin_data) => {
                // Deserialize custom value
                bincode::deserialize::<Box<dyn CustomValue>>(&plugin_data.data)
                    .map(|custom_value| {
                        let value = Value::custom_value(custom_value, plugin_data.span);
                        PipelineData::Value(value, None)
                    })
                    .map_err(|err| ShellError::PluginFailedToDecode {
                        msg: err.to_string(),
                    })
            }
            PipelineDataHeader::ListStream(id) => Ok(make_list_stream(self, id, None)),
            PipelineDataHeader::ExternalStream(id, info) => {
                Ok(make_external_stream(self, id, &info, None))
            }
        }
    }

    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<PipelineData>), ShellError> {
        match data {
            PipelineData::Value(value, _) => {
                let span = value.span();
                match value {
                    // Serialize custom values as PluginData
                    Value::CustomValue { val, .. } => match bincode::serialize(&val) {
                        Ok(data) => {
                            let name = Some(val.value_string());
                            Ok((
                                PipelineDataHeader::PluginData(PluginData { name, data, span }),
                                None,
                            ))
                        }
                        Err(err) => {
                            return Err(ShellError::PluginFailedToEncode {
                                msg: err.to_string(),
                            })
                        }
                    },
                    // Other values can be serialized as-is
                    value => Ok((PipelineDataHeader::Value(value), None)),
                }
            }
            PipelineData::ListStream(..) => Ok((
                PipelineDataHeader::ListStream(self.new_stream_id()?),
                Some(data),
            )),
            PipelineData::ExternalStream {
                ref stdout,
                ref stderr,
                ref exit_code,
                span,
                trim_end_newline,
                ..
            } => {
                // Gather info from the stream
                let info = ExternalStreamInfo {
                    span,
                    stdout: stdout.as_ref().map(RawStreamInfo::from),
                    stderr: stderr.as_ref().map(RawStreamInfo::from),
                    has_exit_code: exit_code.is_some(),
                    trim_end_newline,
                };
                Ok((
                    PipelineDataHeader::ExternalStream(self.new_stream_id()?, info),
                    Some(data),
                ))
            }
            PipelineData::Empty => Ok((PipelineDataHeader::Empty, None)),
        }
    }
}

/// A reference through which the nushell engine can be interacted with during execution.
#[derive(Clone)]
pub struct EngineInterface {
    io: Arc<dyn EngineInterfaceIo>,
}

impl<R, W> From<EngineInterfaceImpl<R, W>> for EngineInterface
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn from(engine_impl: EngineInterfaceImpl<R, W>) -> Self {
        let arc = Arc::new(engine_impl);
        EngineInterface { io: arc.clone() }
    }
}

impl EngineInterface {
    /// Create the engine interface from the given reader, writer, and encoder.
    pub(crate) fn new<R, W, E>(reader: R, writer: W, encoder: E) -> EngineInterface
    where
        R: BufRead + Send + 'static,
        W: Write + Send + 'static,
        E: PluginEncoder + 'static,
    {
        EngineInterfaceImpl::new((reader, encoder.clone()), (writer, encoder)).into()
    }

    /// Read a plugin call from the engine
    pub(crate) fn read_call(&self) -> Result<Option<PluginCall>, ShellError> {
        self.io.read_call()
    }

    /// Create [PipelineData] appropriate for the given [PipelineDataHeader].
    pub(crate) fn make_pipeline_data(
        &self,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError> {
        self.io.clone().make_pipeline_data(header)
    }

    /// Write a plugin call response back to the engine
    pub(crate) fn write_call_response(
        &self,
        response: PluginCallResponse,
    ) -> Result<(), ShellError> {
        self.io.write_call_response(response)
    }

    /// Write a response appropriate for the given [PipelineData] and consume the stream(s) to
    /// completion, if any.
    pub(crate) fn write_pipeline_data_response(
        &self,
        data: PipelineData,
    ) -> Result<(), ShellError> {
        match self.io.make_pipeline_data_header(data)? {
            (header, Some(data)) => {
                // Write the full stream of data
                self.io
                    .write_call_response(PluginCallResponse::PipelineData(header.clone()))?;
                self.io.write_pipeline_data_stream(&header, data)
            }
            (header, None) => {
                // Just the response
                self.io
                    .write_call_response(PluginCallResponse::PipelineData(header))
            }
        }
    }
}
