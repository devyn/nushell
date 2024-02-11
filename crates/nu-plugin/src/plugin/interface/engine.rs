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
        PluginData, PluginInput, PluginOutput, RawStreamInfo, StreamData, StreamId, EngineCallId, EngineCallResponse, EngineCall,
    },
};

use super::{
    buffers::StreamBuffers,
    make_external_stream, make_list_stream,
    stream_data_io::{impl_stream_data_io, StreamDataIo, StreamDataIoExt},
    PluginRead, PluginWrite, next_id_from,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct EngineInterfaceImpl<R, W> {
    // Always lock read and then write mutex, if using both
    read: Mutex<ReadPart<R>>,
    write: Mutex<W>,
    /// The next available stream id
    next_stream_id: AtomicUsize,
    /// The next available engine call id
    next_engine_call_id: AtomicUsize,
}

#[derive(Debug)]
struct ReadPart<R> {
    reader: R,
    /// Stores stream messages that can't be handled immediately
    stream_buffers: StreamBuffers,
    /// Lists engine calls that are expecting a response. If a response is received out of order
    /// it can be placed in here for the waiting thread to pick up later
    pending_engine_calls: Vec<(EngineCallId, Option<EngineCallResponse>)>,
}

impl<R, W> EngineInterfaceImpl<R, W> {
    pub(crate) fn new(reader: R, writer: W) -> EngineInterfaceImpl<R, W> {
        EngineInterfaceImpl {
            read: Mutex::new(ReadPart {
                reader,
                stream_buffers: StreamBuffers::default(),
                pending_engine_calls: vec![],
            }),
            write: Mutex::new(writer),
            next_stream_id: AtomicUsize::new(0),
            next_engine_call_id: AtomicUsize::new(0),
        }
    }
}

impl<R> ReadPart<R> where R: PluginRead {
    /// Handle an out of order message
    fn handle_out_of_order(
        &mut self,
        _io: &Arc<EngineInterfaceImpl<R, impl PluginWrite>>,
        msg: PluginInput
    ) -> Result<(), ShellError> {
        match msg {
            PluginInput::Call(_) => Err(ShellError::PluginFailedToDecode {
                msg: "unexpected Call in this context - possibly nested".into(),
            }),
            // Store any out of order stream data in the stream buffers
            PluginInput::StreamData(id, data) => self.stream_buffers.skip(id, data),
            PluginInput::EngineCallResponse(id, response) => {
                let (_, response_hole) = self.pending_engine_calls.iter_mut()
                    .find(|(call_id, _)| id == *call_id)
                    .ok_or_else(|| ShellError::PluginFailedToDecode {
                        msg: format!("unexpected EngineCallResponse id={id}")
                    })?;

                if response_hole.is_none() {
                    *response_hole = Some(response);
                    Ok(())
                } else {
                    Err(ShellError::PluginFailedToDecode {
                        msg: format!("received multiple responses for engine call id={id}")
                    })
                }
            },
        }
    }
}

// Implement the stream handling methods (see StreamDataIo).
impl_stream_data_io!(
    EngineInterfaceImpl,
    PluginInput(read_input),
    PluginOutput(write_output)
);

/// The trait indirection is so that we can hide the types with a trait object inside
/// EngineInterface. As such, this trait must remain object safe.
pub(crate) trait EngineInterfaceIo: StreamDataIo {
    fn read_call(self: Arc<Self>) -> Result<Option<PluginCall>, ShellError>;
    fn write_call_response(&self, response: PluginCallResponse) -> Result<(), ShellError>;

    fn write_engine_call(&self, call: EngineCall) -> Result<(), ShellError>;
    fn read_engine_call_response(self: Arc<Self>) -> Result<EngineCallResponse, ShellError>;

    /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
    fn make_pipeline_data(
        self: Arc<Self>,
        header: PipelineDataHeader,
    ) -> Result<PipelineData, ShellError>;

    /// Create a valid header to send the given [`PipelineData`].
    ///
    /// Returns the header, and the arguments to be sent to `write_pipeline_data_stream`
    /// if necessary.
    fn make_pipeline_data_header(
        &self,
        data: PipelineData,
    ) -> Result<(PipelineDataHeader, Option<(PipelineDataHeader, PipelineData)>), ShellError>;
}

impl<R, W> EngineInterfaceIo for EngineInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    fn read_call(self: Arc<Self>) -> Result<Option<PluginCall>, ShellError> {
        loop {
            let mut read = self.read.lock().expect("read mutex poisoned");
            match read.reader.read_input()? {
                Some(PluginInput::Call(call)) => {
                    // Check the call input type to set the stream buffers up
                    if let PluginCall::Run(CallInfo { ref input, .. }) = call {
                        read.stream_buffers.init_stream(input)?;
                    }
                    return Ok(Some(call));
                }
                // Handle some other message
                Some(other) => read.handle_out_of_order(&self, other)?,
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

    fn write_engine_call(&self, call: EngineCall) -> Result<(), ShellError> {
        todo!()
    }

    fn read_engine_call_response(self: Arc<Self>) -> Result<EngineCallResponse, ShellError> {
        todo!()
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
    ) -> Result<(PipelineDataHeader, Option<(PipelineDataHeader, PipelineData)>), ShellError> {
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
            PipelineData::ListStream(..) => {
                let header = PipelineDataHeader::ListStream(self.new_stream_id()?);
                Ok((
                    header.clone(),
                    Some((header, data)),
                ))
            }
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
                let header = PipelineDataHeader::ExternalStream(self.new_stream_id()?, info);
                Ok((
                    header.clone(),
                    Some((header, data)),
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
        Arc::new(engine_impl).into()
    }
}

impl<T> From<Arc<T>> for EngineInterface where T: EngineInterfaceIo + 'static {
    fn from(value: Arc<T>) -> Self {
        EngineInterface {
            io: value
        }
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
        self.io.clone().read_call()
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
