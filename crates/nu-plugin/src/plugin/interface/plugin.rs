//! Interface used by the engine to communicate with the plugin.

use std::{
    io::{BufRead, Write},
    marker::PhantomData,
    sync::{atomic::{AtomicUsize, AtomicBool}, Arc, Mutex, MutexGuard, mpsc}, collections::VecDeque,
};

use nu_protocol::{PipelineData, ShellError, Value, PluginSignature, Spanned, engine::Closure};

use crate::{
    plugin::{context::PluginExecutionContext, PluginEncoder},
    protocol::{
        EngineCall, EngineCallId, EngineCallResponse, ExternalStreamInfo, ListStreamInfo,
        PipelineDataHeader, PluginCall, PluginCallResponse, PluginCustomValue, PluginData,
        PluginInput, PluginOutput, RawStreamInfo, StreamId, PluginCallId, CallInfo,
    }, sequence::Sequence, LabeledError,
};

use super::{
    PluginRead, PluginWrite, stream::{StreamManager, StreamManagerHandle}, InterfaceManager, Interface,
};

// #[cfg(test)]
// mod tests;

#[derive(Debug)]
enum ReceivedPluginCallMessage {
    /// The final response to send
    Response(PluginCallResponse<PipelineData>),

    /// An engine call that should be evaluated and responded to, but is not the final response
    ///
    /// We send this back to the thread that made the plugin call so we don't block the reader
    /// thread
    EngineCall(EngineCallId, EngineCall<PipelineData>),
}

/// Context for plugin call execution
#[derive(Clone)]
struct Context(Arc<dyn PluginExecutionContext>);

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Context")
    }
}

impl std::ops::Deref for Context {
    type Target = dyn PluginExecutionContext;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Internal shared state between the manager and each interface.
struct PluginInterfaceState {
    /// Sequence for generating plugin call ids
    plugin_call_id_sequence: Sequence,
    /// Sequence for generating stream ids
    stream_id_sequence: Sequence,
    /// Channels waiting for a response to a plugin call
    plugin_call_response_senders: Mutex<Vec<(PluginCallId, mpsc::Sender<ReceivedPluginCallMessage>)>>,
    /// Contexts for plugin calls
    contexts: Mutex<Vec<(PluginCallId, Context)>>,
    /// The synchronized output writer
    writer: Box<dyn PluginWrite>,
}

impl std::fmt::Debug for PluginInterfaceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginInterfaceState")
            .field("plugin_call_id_sequence", &self.plugin_call_id_sequence)
            .field("stream_id_sequence", &self.stream_id_sequence)
            .field("plugin_call_response_senders", &self.plugin_call_response_senders)
            .field("contexts", &self.contexts)
            .finish_non_exhaustive()
    }
}

impl PluginInterfaceState {
    fn lock_plugin_call_response_senders(&self) -> Result<MutexGuard<Vec<(usize, mpsc::Sender<ReceivedPluginCallMessage>)>>, ShellError> {
        self.plugin_call_response_senders.lock().map_err(|_| ShellError::NushellFailed {
            msg: "plugin_call_response_senders mutex poisoned due to panic".into()
        })
    }

    fn lock_contexts(&self) -> Result<MutexGuard<Vec<(PluginCallId, Context)>>, ShellError> {
        self.contexts.lock().map_err(|_| ShellError::NushellFailed {
            msg: "contexts mutex poisoned due to panic".into(),
        })
    }

    fn get_context(&self, id: PluginCallId) -> Result<Context, ShellError> {
        self.lock_contexts()?
            .iter()
            .find(|(context_id, _)| *context_id == id)
            .map(|(_, context)| context.clone())
            .ok_or_else(|| ShellError::PluginFailedToDecode {
                msg: format!("Context not found for plugin call id={id}"),
            })
    }

    fn add_context(&self, id: PluginCallId, context: Context) -> Result<(), ShellError> {
        self.lock_contexts()?.push((id, context));
        Ok(())
    }

    fn remove_context(&self, id: PluginCallId) -> Result<Context, ShellError> {
        let mut contexts = self.lock_contexts()?;
        if let Some(index) = contexts.iter().position(|(context_id, _)| *context_id == id) {
            Ok(contexts.swap_remove(index).1)
        } else {
            Err(ShellError::PluginFailedToDecode {
                msg: format!("Context not found for plugin call id={id}")
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct PluginInterfaceManager {
    /// Shared state
    state: Arc<PluginInterfaceState>,
    /// Manages stream messages and state
    stream_manager: StreamManager,
    /// Engine interrupt signal
    ctrlc: Option<Arc<AtomicBool>>,
}

impl PluginInterfaceManager {
    pub(crate) fn new<W>(writer: W, ctrlc: Option<Arc<AtomicBool>>) -> PluginInterfaceManager
    where
        W: PluginWrite,
    {
        PluginInterfaceManager {
            state: Arc::new(PluginInterfaceState {
                plugin_call_id_sequence: Sequence::default(),
                stream_id_sequence: Sequence::default(),
                plugin_call_response_senders: Mutex::new(Vec::new()),
                contexts: Mutex::new(Vec::new()),
                writer: Box::new(writer)
            }),
            stream_manager: StreamManager::new(),
            ctrlc,
        }
    }

    /// Send a [`PluginCallResponse`] to the appropriate sender
    fn send_plugin_call_response(
        &self,
        id: PluginCallId,
        response: PluginCallResponse<PipelineData>,
    ) -> Result<(), ShellError> {
        let mut senders = self
            .state
            .plugin_call_response_senders
            .lock()
            .map_err(|_| ShellError::NushellFailed {
                msg: "plugin_call_response_senders mutex poisoned".into(),
            })?;
        // Remove the sender, since this would be the last message
        if let Some(index) = senders.iter().position(|(sender_id, _)| *sender_id == id) {
            let (_, sender) = senders.swap_remove(index);
            drop(senders);
            if sender.send(ReceivedPluginCallMessage::Response(response)).is_err() {
                log::warn!("Received a plugin call response for id={id}, but the caller hung up");
            }
            Ok(())
        } else {
            Err(ShellError::PluginFailedToDecode {
                msg: format!("Unknown plugin call ID: {id}"),
            })
        }
    }

    /// Send an [`EngineCall`] to the appropriate sender
    fn send_engine_call(
        &self,
        plugin_call_id: PluginCallId,
        engine_call_id: EngineCallId,
        call: EngineCall<PipelineData>,
    ) -> Result<(), ShellError> {
        let senders = self
            .state
            .plugin_call_response_senders
            .lock()
            .map_err(|_| ShellError::NushellFailed {
                msg: "plugin_call_response_senders mutex poisoned".into(),
            })?;
        // Don't remove the sender, as there could be more calls or responses
        if let Some((_, sender)) = senders.iter().find(|(id, _)| *id == plugin_call_id) {
            if sender.send(ReceivedPluginCallMessage::EngineCall(engine_call_id, call)).is_err() {
                drop(senders);
                log::warn!("Received an engine call for plugin_call_id={plugin_call_id}, \
                    but the caller hung up");
                // We really have no choice here but to send the response ourselves and hope we
                // don't block
                self.state.writer.write_input(
                    &PluginInput::EngineCallResponse(
                        engine_call_id,
                        EngineCallResponse::Error(ShellError::IOError {
                            msg: "Can't make engine call because the original caller hung up".into(),
                        }),
                    )
                )?;
            }
            Ok(())
        } else {
            Err(ShellError::PluginFailedToDecode {
                msg: format!("Unknown plugin call ID: {plugin_call_id}"),
            })
        }
    }

    /// True if there are no other copies of the state (which would mean there are no interfaces
    /// and no stream readers/writers)
    pub(crate) fn is_finished(&self) -> bool {
        Arc::strong_count(&self.state) < 2
    }

    /// Loop on input from the given reader as long as `is_finished()` is false
    pub(crate) fn consume_all<R: PluginRead>(&mut self, mut reader: R) -> Result<(), ShellError> {
        while let Some(msg) = reader.read_output()? {
            if self.is_finished() {
                break;
            }

            self.consume(msg)?;
        }
        Ok(())
    }
}

impl InterfaceManager for PluginInterfaceManager {
    type Interface = PluginInterface;
    type Input = PluginOutput;
    type Context = Context;

    fn get_interface(&self) -> Self::Interface {
        PluginInterface {
            state: self.state.clone(),
            stream_manager_handle: self.stream_manager.get_handle(),
        }
    }

    fn consume(&mut self, input: Self::Input) -> Result<(), ShellError> {
        match input {
            PluginOutput::Stream(message) => self.consume_stream_message(message),
            PluginOutput::CallResponse(id, response) => {
                // Handle reading the pipeline data, if any
                let response = match response {
                    PluginCallResponse::Error(err) => PluginCallResponse::Error(err),
                    PluginCallResponse::Signature(sigs) => PluginCallResponse::Signature(sigs),
                    PluginCallResponse::PipelineData(data) => {
                        // If there's an error with initializing this stream, change it to a plugin
                        // error response, but send it anyway
                        match self.read_pipeline_data(data, &self.state.get_context(id)?) {
                            Ok(data) => PluginCallResponse::PipelineData(data),
                            Err(err) => PluginCallResponse::Error(err.into()),
                        }
                    }
                };
                self.send_plugin_call_response(id, response)
            }
            PluginOutput::EngineCall { context, id, call } => {
                // Handle reading the pipeline data, if any
                let exec_context = self.state.get_context(context)?;
                let call = match call {
                    EngineCall::GetConfig => Ok(EngineCall::GetConfig),
                    EngineCall::EvalClosure {
                        closure,
                        positional,
                        input,
                        redirect_stdout,
                        redirect_stderr,
                    } => {
                        self.read_pipeline_data(input, &exec_context)
                            .map(|input| EngineCall::EvalClosure {
                                closure,
                                positional,
                                input,
                                redirect_stdout,
                                redirect_stderr,
                            })
                    }
                };
                match call {
                    Ok(call) => self.send_engine_call(context, id, call),
                    // If there was an error with setting up the call, just write the error
                    Err(err) => self.get_interface().write_engine_call_response(
                        id,
                        EngineCallResponse::Error(err),
                        &Some(exec_context),
                    )
                }
            }
        }
    }

    fn value_from_plugin_data(
        &self,
        plugin_data: PluginData,
        context: &Context,
    ) -> Result<Value, ShellError> {
        // Convert to PluginCustomData
        Ok(Value::custom_value(
            Box::new(PluginCustomValue {
                name: plugin_data
                    .name
                    .ok_or_else(|| ShellError::PluginFailedToDecode {
                        msg: "String representation of PluginData not provided".into(),
                    })?,
                data: plugin_data.data,
                filename: context.filename().to_owned(),
                shell: context.shell().map(|p| p.to_owned()),
                source: context.command_name().to_owned(),
            }),
            plugin_data.span,
        ))
    }

    fn ctrlc(&self, context: &Context) -> Option<Arc<AtomicBool>> {
        context.ctrlc().cloned()
    }

    fn stream_manager(&self) -> &StreamManager {
        &self.stream_manager
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PluginInterface {
    /// Shared state
    state: Arc<PluginInterfaceState>,
    /// Handle to stream manager
    stream_manager_handle: StreamManagerHandle,
}

impl PluginInterface {
    /// Write an [`EngineCallResponse`]. Writes the full stream contained in any [`PipelineData`]
    /// before returning.
    pub(crate) fn write_engine_call_response(
        &self,
        id: EngineCallId,
        response: EngineCallResponse<PipelineData>,
        context: &Option<Context>,
    ) -> Result<(), ShellError> {
        // Set up any stream if necessary
        let (response, writer) = match response {
            EngineCallResponse::PipelineData(data) => {
                let (header, writer) = self.init_write_pipeline_data(data, context)?;
                (EngineCallResponse::PipelineData(header), Some(writer))
            }
            // No pipeline data:
            EngineCallResponse::Error(err) => (EngineCallResponse::Error(err), None),
            EngineCallResponse::Config(config) => (EngineCallResponse::Config(config), None),
        };

        // Write the response, including the pipeline data header if present
        self.write(PluginInput::EngineCallResponse(id, response))?;

        // If we have a stream to write, do it now
        if let Some(writer) = writer {
            writer.write()?;
        }

        Ok(())
    }

    /// Perform a plugin call. Input and output streams are handled, and engine calls are handled
    /// too if there are any before the final response.
    fn plugin_call(
        &self,
        call: PluginCall<PipelineData>,
        context: &Option<Context>,
    ) -> Result<PluginCallResponse<PipelineData>, ShellError> {
        let id = self.state.plugin_call_id_sequence.next()?;
        let (tx, rx) = mpsc::channel();

        // Convert the call into one with a header and handle the stream, if necessary
        let (call, writer) = match call {
            PluginCall::Signature => (PluginCall::Signature, None),
            PluginCall::CollapseCustomValue(value) => (PluginCall::CollapseCustomValue(value), None),
            PluginCall::Run(CallInfo { name, call, input, config }) => {
                let (header, writer) = self.init_write_pipeline_data(input, context)?;
                (
                    PluginCall::Run(CallInfo {
                        name,
                        call,
                        input: header,
                        config,
                    }),
                    Some(writer),
                )
            }
        };

        // Register the channel
        self.state.lock_plugin_call_response_senders()?.push((id, tx));

        // Write request
        self.write(PluginInput::Call(id, call))?;

        // Finish writing stream, if present
        if let Some(writer) = writer {
            writer.write()?;
        }

        // Handle messages from receiver
        for msg in rx {
            match msg {
                ReceivedPluginCallMessage::Response(resp) => {
                    return Ok(resp);
                }
                ReceivedPluginCallMessage::EngineCall(engine_call_id, engine_call) => {
                    let resp = handle_engine_call(engine_call, context)
                        .unwrap_or_else(EngineCallResponse::Error);
                    // Handle stream
                    let (resp, writer) = match resp {
                        EngineCallResponse::Error(error) =>
                            (EngineCallResponse::Error(error), None),
                        EngineCallResponse::Config(config) =>
                            (EngineCallResponse::Config(config), None),
                        EngineCallResponse::PipelineData(data) => {
                            match self.init_write_pipeline_data(data, context) {
                                Ok((header, writer)) => (
                                    EngineCallResponse::PipelineData(header),
                                    Some(writer),
                                ),
                                // just respond with the error if we fail to set it up
                                Err(err) => (
                                    EngineCallResponse::Error(err),
                                    None,
                                )
                            }
                        }
                    };
                    // Write the response, then the stream
                    self.write(PluginInput::EngineCallResponse(engine_call_id, resp))?;
                    if let Some(writer) = writer {
                        writer.write()?;
                    }
                }
            }
        }
        // If we fail to get a response
        Err(ShellError::PluginFailedToDecode {
            msg: "Failed to receive response to plugin call".into(),
        })
    }
}

impl Interface for PluginInterface {
    type Output = PluginInput;
    type Context = Option<Context>;

    fn write(&self, output: Self::Output) -> Result<(), ShellError> {
        self.state.writer.write_input(&output)
    }

    fn stream_id_sequence(&self) -> &Sequence {
        &self.state.stream_id_sequence
    }

    fn stream_manager_handle(&self) -> &StreamManagerHandle {
        &self.stream_manager_handle
    }

    fn value_to_plugin_data(
        &self,
        value: &Value,
        context: &Option<Context>,
    ) -> Result<Option<PluginData>, ShellError> {
        if let Value::CustomValue { val, .. } = value {
            let context = context.as_ref().ok_or_else(|| ShellError::PluginFailedToEncode {
                msg: "Can't handle PluginCustomValue without knowing the context".into(),
            })?;
            match val.as_any().downcast_ref::<PluginCustomValue>() {
                Some(plugin_data) if plugin_data.filename == context.filename() => {
                    Ok(Some(
                        PluginData {
                            name: None, // plugin doesn't need it.
                            data: plugin_data.data.clone(),
                            span: value.span(),
                        }
                    ))
                }
                _ => {
                    let custom_value_name = val.value_string();
                    Err(ShellError::GenericError {
                        error: format!(
                            "Plugin {} can not handle the custom value {}",
                            context.command_name(),
                            custom_value_name
                        ),
                        msg: format!("custom value {custom_value_name}"),
                        span: Some(value.span()),
                        help: None,
                        inner: vec![],
                    })
                }
            }
        } else {
            Ok(None)
        }
    }
}

/// Handle an engine call.
pub(crate) fn handle_engine_call(
    call: EngineCall<PipelineData>,
    context: &Option<Context>,
) -> Result<EngineCallResponse<PipelineData>, ShellError> {
    let require_context = || context.as_ref().ok_or_else(|| ShellError::GenericError {
        error: "A plugin execution context is required for this engine call".into(),
        msg: format!("attempted to call {} outside of a command invocation", call.name()),
        span: None,
        help: Some("this is probably a bug with the plugin".into()),
        inner: vec![],
    });
    match call {
        EngineCall::GetConfig => {
            let context = require_context()?;
            let config = Box::new(context.get_config().clone());
            Ok(EngineCallResponse::Config(config))
        }
        EngineCall::EvalClosure {
            closure,
            positional,
            input,
            redirect_stdout,
            redirect_stderr,
        } => {
            require_context()?.eval_closure(
                closure,
                positional,
                input,
                redirect_stdout,
                redirect_stderr,
            ).map(EngineCallResponse::PipelineData)
        }
    }
} 

/*
impl<R, W> StreamDataIoBase for PluginInterfaceImpl<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    type ReadPart = ReadPart<R, W>;
    type WritePart = WritePart<W>;

    fn lock_read(&self) -> Result<MutexGuard<ReadPart<R, W>>, ShellError> {
        let read = self.read.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in PluginInterface: read mutex poisoned due to panic".into(),
        })?;

        if let Some(ref err) = read.error {
            // Propagate an error
            Err((**err).clone())
        } else {
            Ok(read)
        }
    }

    fn lock_write(&self) -> Result<MutexGuard<WritePart<W>>, ShellError> {
        self.write.lock().map_err(|_| ShellError::NushellFailed {
            msg: "error in PluginInterface: write mutex poisoned due to panic".into(),
        })
    }

    fn new_stream_id(&self) -> Result<StreamId, ShellError> {
        next_id_from(&self.next_stream_id)
    }
}

impl<R, W> StreamDataRead for ReadPart<R, W>
where
    R: PluginRead + 'static,
    W: PluginWrite + 'static,
{
    type Message = PluginOutput;
    type Base = PluginInterfaceImpl<R, W>;

    fn read(&mut self) -> Result<Option<Self::Message>, ShellError> {
        self.reader.read_output()
    }

    fn stream_buffers(&mut self) -> &mut StreamBuffers {
        &mut self.stream_buffers
    }

    fn interrupt_flags(&mut self) -> &mut StreamInterruptFlags {
        &mut self.interrupt_flags
    }

    // Default message handler
    fn handle_message(
        &mut self,
        io: &Arc<Self::Base>,
        msg: PluginOutput,
    ) -> Result<(), ShellError> {
        match msg {
            // Store an out of order call response
            PluginOutput::CallResponse(response) => {
                self.responses.push_back(response);
                Ok(())
            }
            // Handle out of order stream messages
            PluginOutput::Stream(stream_msg) => {
                self.handle_out_of_order(stream_msg)
            }
            // Execute an engine call during plugin execution
            PluginOutput::EngineCall(id, engine_call) => {
                io.clone().handle_engine_call(id, engine_call)
            }
        }
    }
}

impl<W> StreamDataWrite for WritePart<W>
where
    W: PluginWrite,
{
    type Message = PluginInput;

    fn write(&mut self, msg: Self::Message) -> Result<(), ShellError> {
        self.writer.write_input(&msg)
    }

    fn flush(&mut self) -> Result<(), ShellError> {
        self.writer.flush()
    }
}
*/

// /// The trait indirection is so that we can hide the types with a trait object inside
// /// PluginInterface. As such, this trait must remain object safe.
// pub(crate) trait PluginInterfaceIo: Send + Sync {
//     fn context(&self) -> Option<&Arc<dyn PluginExecutionContext>>;

//     /// Propagate an error to future attempts to read from the interface.
//     fn set_read_failed(&self, err: ShellError);

//     /// Write a [PluginCall] to the plugin.
//     fn write_call(&self, call: PluginCall) -> Result<(), ShellError>;

//     /// Read the response to a [PluginCall].
//     fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError>;

//     /// Write an [EngineCallResponse] to the engine call with the given `id`.
//     fn write_engine_call_response(
//         &self,
//         id: EngineCallId,
//         response: EngineCallResponse,
//     ) -> Result<(), ShellError>;

//     /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
//     ///
//     /// Error if [PluginExecutionContext] was not provided when creating the interface.
//     fn make_pipeline_data(
//         self: Arc<Self>,
//         header: PipelineDataHeader,
//     ) -> Result<PipelineData, ShellError>;

//     /// Create a valid header to send the given [`PipelineData`].
//     ///
//     /// Returns the header, and the arguments to be sent with `write_pipeline_data_stream`
//     /// if necessary.
//     ///
//     /// Error if [PluginExecutionContext] was not provided when creating the interface.
//     fn make_pipeline_data_header(
//         &self,
//         data: PipelineData,
//     ) -> Result<
//         (
//             PipelineDataHeader,
//             Option<(PipelineDataHeader, PipelineData)>,
//         ),
//         ShellError,
//     >;

//     /// Handle an [`EngineCall`] during execution.
//     fn handle_engine_call(
//         self: Arc<Self>,
//         id: EngineCallId,
//         engine_call: EngineCall,
//     ) -> Result<(), ShellError>;
// }

// impl<R, W> PluginInterfaceIo for PluginInterfaceImpl<R, W>
// where
//     R: PluginRead + 'static,
//     W: PluginWrite + 'static,
// {
//     fn context(&self) -> Option<&Arc<dyn PluginExecutionContext>> {
//         self.context.as_ref()
//     }

//     fn set_read_failed(&self, err: ShellError) {
//         // If we fail to unlock, it will cause an error somewhere anyway
//         if let Ok(mut guard) = self.read.lock() {
//             guard.error = Some(err.into());
//         }
//     }

//     fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
//         // let mut write = self.lock_write()?;
//         // log::trace!("Writing plugin call: {call:?}");

//         // if !write.waiting_for_call_response {
//         //     write.write(PluginInput::Call(call))?;
//         //     write.flush()?;

//         //     log::trace!("Wrote plugin call");
//         //     Ok(())
//         // } else {
//         //     Err(ShellError::NushellFailed {
//         //         msg: "Attempted to make another call to the plugin before it responded".into(),
//         //     })
//         // }
//         todo!()
//     }

//     fn read_call_response(self: Arc<Self>) -> Result<PluginCallResponse, ShellError> {
//         log::trace!("Reading plugin call response");

//         // loop {
//         //     let mut read = self.lock_read()?;
//         //     // Check if a call response was handled out of order
//         //     let response =
//         //         if let Some(response) = read.responses.pop_front() {
//         //             response
//         //         } else {
//         //             match read.read()? {
//         //                 Some(PluginOutput::CallResponse(response)) => response,
//         //                 // Handle some other message
//         //                 Some(other) => {
//         //                     read.handle_message(&self, other)?;
//         //                     continue;
//         //                 }
//         //                 // End of input
//         //                 None => {
//         //                     return Err(ShellError::PluginFailedToDecode {
//         //                         msg: "unexpected end of stream before receiving call response".into(),
//         //                     })
//         //                 }
//         //             }
//         //         };
//         //     // Check the call input type to set the stream buffers up
//         //     if let PluginCallResponse::PipelineData(header) = &response {
//         //         read.stream_buffers.init_stream(header)?;
//         //     }
//         //     // Reset the flag so that another plugin call could be made later
//         //     drop(read);
//         //     self.lock_write()?.waiting_for_call_response = false;

//         //     return Ok(response);
//         // }
//         todo!()
//     }

//     fn write_engine_call_response(
//         &self,
//         id: EngineCallId,
//         response: EngineCallResponse,
//     ) -> Result<(), ShellError> {
//         // let mut write = self.lock_write()?;
//         log::trace!("Writing engine call response for id={id}: {response:?}");

//         // write.write(PluginInput::EngineCallResponse(id, response))?;
//         // write.flush()?;

//         log::trace!("Wrote engine call response for id={id}");
//         todo!()
//     }

//     fn make_pipeline_data(
//         self: Arc<Self>,
//         header: PipelineDataHeader,
//     ) -> Result<PipelineData, ShellError> {
//         // let context = self.context().ok_or_else(|| ShellError::NushellFailed {
//         //     msg: "PluginExecutionContext must be provided to call make_pipeline_data".into(),
//         // })?;

//         // match header {
//         //     PipelineDataHeader::Empty => Ok(PipelineData::Empty),
//         //     PipelineDataHeader::Value(value) => Ok(PipelineData::Value(value, None)),
//         //     PipelineDataHeader::PluginData(plugin_data) => {
//         //         // Convert to PluginCustomData
//         //         let value = Value::custom_value(
//         //             Box::new(PluginCustomValue {
//         //                 name: plugin_data
//         //                     .name
//         //                     .ok_or_else(|| ShellError::PluginFailedToDecode {
//         //                         msg: "String representation of PluginData not provided".into(),
//         //                     })?,
//         //                 data: plugin_data.data,
//         //                 filename: context.filename().to_owned(),
//         //                 shell: context.shell().map(|p| p.to_owned()),
//         //                 source: context.command_name().to_owned(),
//         //             }),
//         //             plugin_data.span,
//         //         );
//         //         Ok(PipelineData::Value(value, None))
//         //     }
//         //     PipelineDataHeader::ListStream(info) => {
//         //         let ctrlc = context.ctrlc().cloned();
//         //         Ok(make_pipe_list_stream(self, &info, ctrlc))
//         //     }
//         //     PipelineDataHeader::ExternalStream(info) => {
//         //         let ctrlc = context.ctrlc().cloned();
//         //         Ok(make_pipe_external_stream(self, &info, ctrlc))
//         //     }
//         // }
//         todo!()
//     }

//     fn make_pipeline_data_header(
//         &self,
//         data: PipelineData,
//     ) -> Result<
//         (
//             PipelineDataHeader,
//             Option<(PipelineDataHeader, PipelineData)>,
//         ),
//         ShellError,
//     > {
//         // let context = self.context().ok_or_else(|| ShellError::NushellFailed {
//         //     msg: "PluginExecutionContext must be provided to call make_pipeline_data_header".into(),
//         // })?;

//         // match data {
//         //     PipelineData::Value(ref value @ Value::CustomValue { ref val, .. }, _) => {
//         //         match val.as_any().downcast_ref::<PluginCustomValue>() {
//         //             Some(plugin_data) if plugin_data.filename == context.filename() => {
//         //                 Ok((
//         //                     PipelineDataHeader::PluginData(PluginData {
//         //                         name: None, // plugin doesn't need it.
//         //                         data: plugin_data.data.clone(),
//         //                         span: value.span(),
//         //                     }),
//         //                     None,
//         //                 ))
//         //             }
//         //             _ => {
//         //                 let custom_value_name = val.value_string();
//         //                 Err(ShellError::GenericError {
//         //                     error: format!(
//         //                         "Plugin {} can not handle the custom value {}",
//         //                         context.command_name(),
//         //                         custom_value_name
//         //                     ),
//         //                     msg: format!("custom value {custom_value_name}"),
//         //                     span: Some(value.span()),
//         //                     help: None,
//         //                     inner: vec![],
//         //                 })
//         //             }
//         //         }
//         //     }
//         //     PipelineData::Value(Value::LazyRecord { ref val, .. }, _) => {
//         //         Ok((PipelineDataHeader::Value(val.collect()?), None))
//         //     }
//         //     PipelineData::Value(value, _) => Ok((PipelineDataHeader::Value(value), None)),
//         //     PipelineData::ListStream(_, _) => {
//         //         let header = PipelineDataHeader::ListStream(ListStreamInfo {
//         //             id: self.new_stream_id()?,
//         //         });
//         //         Ok((header.clone(), Some((header, data))))
//         //     }
//         //     PipelineData::ExternalStream {
//         //         span,
//         //         ref stdout,
//         //         ref stderr,
//         //         ref exit_code,
//         //         trim_end_newline,
//         //         ..
//         //     } => {
//         //         let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
//         //             span,
//         //             stdout: if let Some(ref stdout) = stdout {
//         //                 Some(RawStreamInfo::new(self.new_stream_id()?, stdout))
//         //             } else {
//         //                 None
//         //             },
//         //             stderr: if let Some(ref stderr) = stderr {
//         //                 Some(RawStreamInfo::new(self.new_stream_id()?, stderr))
//         //             } else {
//         //                 None
//         //             },
//         //             exit_code: if exit_code.is_some() {
//         //                 Some(ListStreamInfo {
//         //                     id: self.new_stream_id()?,
//         //                 })
//         //             } else {
//         //                 None
//         //             },
//         //             trim_end_newline,
//         //         });
//         //         Ok((header.clone(), Some((header, data))))
//         //     }
//         //     PipelineData::Empty => Ok((PipelineDataHeader::Empty, None)),
//         // }
//         todo!()
//     }

//     fn handle_engine_call(
//         self: Arc<Self>,
//         id: EngineCallId,
//         engine_call: EngineCall,
//     ) -> Result<(), ShellError> {
//         // let context = self.context().ok_or_else(|| ShellError::NushellFailed {
//         //     msg: "PluginExecutionContext was not provided before making an engine call".into(),
//         // })?;

//         // log::trace!("Handling engine call id={id}: {engine_call:?}");

//         // match engine_call {
//         //     EngineCall::GetConfig => {
//         //         let config = context.get_config().clone().into();
//         //         let response = EngineCallResponse::Config(config);
//         //         self.write_engine_call_response(id, response)
//         //     }
//         //     EngineCall::EvalClosure {
//         //         closure,
//         //         positional,
//         //         input,
//         //         redirect_stdout,
//         //         redirect_stderr,
//         //     } => {
//         //         // Build the input PipelineData
//         //         let input = self.clone().make_pipeline_data(input)?;
//         //         // Evaluate the closure
//         //         match context.eval_closure(
//         //             closure,
//         //             positional,
//         //             input,
//         //             redirect_stdout,
//         //             redirect_stderr,
//         //         ) {
//         //             Ok(output) => {
//         //                 let (header, rest) = self.make_pipeline_data_header(output)?;
//         //                 self.write_engine_call_response(
//         //                     id,
//         //                     EngineCallResponse::PipelineData(header),
//         //                 )?;
//         //                 // Write the stream if necessary
//         //                 if let Some((header, data)) = rest {
//         //                     self.write_pipeline_data_stream(&header, data)
//         //                 } else {
//         //                     Ok(())
//         //                 }
//         //             }
//         //             Err(err) => self.write_engine_call_response(id, EngineCallResponse::Error(err)),
//         //         }
//         //     }
//         // }
//         todo!()
//     }
// }

// /// Implements communication and stream handling for a plugin instance.
// #[derive(Clone)]
// pub(crate) struct PluginInterface {
//     io: Arc<dyn PluginInterfaceIo>,
// }

// impl<R, W> From<PluginInterfaceImpl<R, W>> for PluginInterface
// where
//     R: PluginRead + 'static,
//     W: PluginWrite + 'static,
// {
//     fn from(plugin_impl: PluginInterfaceImpl<R, W>) -> Self {
//         Arc::new(plugin_impl).into()
//     }
// }

// impl<T> From<Arc<T>> for PluginInterface
// where
//     T: PluginInterfaceIo + 'static,
// {
//     fn from(value: Arc<T>) -> Self {
//         PluginInterface { io: value }
//     }
// }

// impl PluginInterface {
//     /// Create the plugin interface from the given reader, writer, encoder, and context.
//     pub(crate) fn new<R, W, E>(
//         reader: R,
//         writer: W,
//         encoder: E,
//         context: Option<Arc<dyn PluginExecutionContext>>,
//     ) -> PluginInterface
//     where
//         (R, E): PluginRead + 'static,
//         (W, E): PluginWrite + 'static,
//         E: PluginEncoder,
//     {
//         PluginInterfaceImpl::new((reader, encoder.clone()), (writer, encoder), context).into()
//     }

//     /// Write a [PluginCall] to the plugin.
//     pub(crate) fn write_call(&self, call: PluginCall) -> Result<(), ShellError> {
//         self.io.write_call(call)
//     }

//     /// Read a [PluginCallResponse] back from the plugin.
//     pub(crate) fn read_call_response(&self) -> Result<PluginCallResponse, ShellError> {
//         self.io.clone().read_call_response()
//     }

//     /// Create [PipelineData] appropriate for the given received [PipelineDataHeader].
//     ///
//     /// Error if [PluginExecutionContext] was not provided when creating the interface.
//     pub(crate) fn make_pipeline_data(
//         &self,
//         header: PipelineDataHeader,
//     ) -> Result<PipelineData, ShellError> {
//         self.io.clone().make_pipeline_data(header)
//     }

//     /// Create a valid header to send the given [`PipelineData`].
//     ///
//     /// Returns the header, and the arguments to be sent with `write_pipeline_data_stream`
//     /// if necessary.
//     ///
//     /// Error if [PluginExecutionContext] was not provided when creating the interface.
//     pub(crate) fn make_pipeline_data_header(
//         &self,
//         data: PipelineData,
//     ) -> Result<
//         (
//             PipelineDataHeader,
//             Option<(PipelineDataHeader, PipelineData)>,
//         ),
//         ShellError,
//     > {
//         self.io.make_pipeline_data_header(data)
//     }

//     /// Write the contents of a [PipelineData]. This is a no-op for non-stream data.
//     #[track_caller]
//     pub(crate) fn write_pipeline_data_stream(
//         &self,
//         header: &PipelineDataHeader,
//         data: PipelineData,
//     ) -> Result<(), ShellError> {
//         // self.io.write_pipeline_data_stream(header, data)
//         todo!()
//     }
// }
