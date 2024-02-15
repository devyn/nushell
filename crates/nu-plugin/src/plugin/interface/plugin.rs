//! Interface used by the engine to communicate with the plugin.

use std::sync::{atomic::AtomicBool, mpsc, Arc, Mutex, MutexGuard};

use nu_protocol::{PipelineData, PluginSignature, ShellError, Value};

use crate::{
    plugin::context::PluginExecutionContext,
    protocol::{
        CallInfo, EngineCall, EngineCallId, EngineCallResponse, PluginCall, PluginCallId,
        PluginCallResponse, PluginCustomValue, PluginData, PluginInput, PluginOutput,
    },
    sequence::Sequence,
};

use super::{
    stream::{StreamManager, StreamManagerHandle},
    Interface, InterfaceManager, PluginRead, PluginWrite,
};

// #[cfg(test)]
// mod tests;

#[derive(Debug)]
enum ReceivedPluginCallMessage {
    /// The final response to send
    Response(PluginCallResponse<PipelineData>),

    /// An critical error with the interface
    Error(ShellError),

    /// An engine call that should be evaluated and responded to, but is not the final response
    ///
    /// We send this back to the thread that made the plugin call so we don't block the reader
    /// thread
    EngineCall(EngineCallId, EngineCall<PipelineData>),
}

/// Context for plugin call execution
#[derive(Clone)]
pub(crate) struct Context(Arc<dyn PluginExecutionContext>);

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
    plugin_call_response_senders:
        Mutex<Vec<(PluginCallId, mpsc::Sender<ReceivedPluginCallMessage>)>>,
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
            .field(
                "plugin_call_response_senders",
                &self.plugin_call_response_senders,
            )
            .field("contexts", &self.contexts)
            .finish_non_exhaustive()
    }
}

impl PluginInterfaceState {
    fn lock_plugin_call_response_senders(
        &self,
    ) -> Result<MutexGuard<Vec<(usize, mpsc::Sender<ReceivedPluginCallMessage>)>>, ShellError> {
        self.plugin_call_response_senders
            .lock()
            .map_err(|_| ShellError::NushellFailed {
                msg: "plugin_call_response_senders mutex poisoned due to panic".into(),
            })
    }

    fn lock_contexts(&self) -> Result<MutexGuard<Vec<(PluginCallId, Context)>>, ShellError> {
        self.contexts.lock().map_err(|_| ShellError::NushellFailed {
            msg: "contexts mutex poisoned due to panic".into(),
        })
    }

    fn get_context(&self, id: PluginCallId) -> Result<Option<Context>, ShellError> {
        Ok(self
            .lock_contexts()?
            .iter()
            .find(|(context_id, _)| *context_id == id)
            .map(|(_, context)| context.clone()))
    }

    fn add_context(&self, id: PluginCallId, context: Context) -> Result<(), ShellError> {
        self.lock_contexts()?.push((id, context));
        Ok(())
    }

    fn remove_context(&self, id: PluginCallId) -> Result<Option<Context>, ShellError> {
        let mut contexts = self.lock_contexts()?;
        if let Some(index) = contexts
            .iter()
            .position(|(context_id, _)| *context_id == id)
        {
            Ok(Some(contexts.swap_remove(index).1))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub(crate) struct PluginInterfaceManager {
    /// Shared state
    state: Arc<PluginInterfaceState>,
    /// Manages stream messages and state
    stream_manager: StreamManager,
}

impl PluginInterfaceManager {
    pub(crate) fn new(writer: impl PluginWrite + 'static) -> PluginInterfaceManager {
        PluginInterfaceManager {
            state: Arc::new(PluginInterfaceState {
                plugin_call_id_sequence: Sequence::default(),
                stream_id_sequence: Sequence::default(),
                plugin_call_response_senders: Mutex::new(Vec::new()),
                contexts: Mutex::new(Vec::new()),
                writer: Box::new(writer),
            }),
            stream_manager: StreamManager::new(),
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

            // Can also remove the context if it exists.
            self.state.remove_context(id)?;

            if sender
                .send(ReceivedPluginCallMessage::Response(response))
                .is_err()
            {
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
            if sender
                .send(ReceivedPluginCallMessage::EngineCall(engine_call_id, call))
                .is_err()
            {
                drop(senders);
                log::warn!(
                    "Received an engine call for plugin_call_id={plugin_call_id}, \
                    but the caller hung up"
                );
                // We really have no choice here but to send the response ourselves and hope we
                // don't block
                self.state
                    .writer
                    .write_input(&PluginInput::EngineCallResponse(
                        engine_call_id,
                        EngineCallResponse::Error(ShellError::IOError {
                            msg: "Can't make engine call because the original caller hung up"
                                .into(),
                        }),
                    ))?;
                self.state.writer.flush()?;
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
    ///
    /// Any errors will be propagated to all read streams automatically.
    pub(crate) fn consume_all<R: PluginRead>(&mut self, mut reader: R) -> Result<(), ShellError> {
        while let Some(msg) = reader.read_output()? {
            if self.is_finished() {
                break;
            }

            if let Err(err) = self.consume(msg) {
                // Error to streams
                let _ = self.stream_manager.broadcast_read_error(err.clone());
                // Error to call waiters
                if let Ok(senders) = self.state.lock_plugin_call_response_senders() {
                    for (_, sender) in senders.iter() {
                        let _ = sender.send(ReceivedPluginCallMessage::Error(err.clone()));
                    }
                }
                return Err(err);
            }
        }
        Ok(())
    }
}

impl InterfaceManager for PluginInterfaceManager {
    type Interface = PluginInterface;
    type Input = PluginOutput;
    type Context = Option<Context>;

    fn get_interface(&self) -> Self::Interface {
        PluginInterface {
            state: self.state.clone(),
            stream_manager_handle: self.stream_manager.get_handle(),
        }
    }

    fn consume(&mut self, input: Self::Input) -> Result<(), ShellError> {
        log::trace!("from plugin: {:?}", input);

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
                    } => self.read_pipeline_data(input, &exec_context).map(|input| {
                        EngineCall::EvalClosure {
                            closure,
                            positional,
                            input,
                            redirect_stdout,
                            redirect_stderr,
                        }
                    }),
                };
                match call {
                    Ok(call) => self.send_engine_call(context, id, call),
                    // If there was an error with setting up the call, just write the error
                    Err(err) => self.get_interface().write_engine_call_response(
                        id,
                        EngineCallResponse::Error(err),
                        &exec_context,
                    ),
                }
            }
        }
    }

    fn value_from_plugin_data(
        &self,
        plugin_data: PluginData,
        context: &Option<Context>,
    ) -> Result<Value, ShellError> {
        let context = context
            .as_ref()
            .ok_or_else(|| ShellError::PluginFailedToDecode {
                msg: "Can't handle custom value outside of call context".into(),
            })?;

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

    fn ctrlc(&self, context: &Option<Context>) -> Option<Arc<AtomicBool>> {
        context
            .as_ref()
            .and_then(|context| context.ctrlc().cloned())
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
        self.flush()?;

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

        // Register the context, if provided
        if let Some(context) = context.clone() {
            self.state.add_context(id, context)?;
        }

        // Convert the call into one with a header and handle the stream, if necessary
        let (call, writer) = match call {
            PluginCall::Signature => (PluginCall::Signature, None),
            PluginCall::CollapseCustomValue(value) => {
                (PluginCall::CollapseCustomValue(value), None)
            }
            PluginCall::Run(CallInfo {
                name,
                call,
                input,
                config,
            }) => {
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
        self.state
            .lock_plugin_call_response_senders()?
            .push((id, tx));

        // Write request
        self.write(PluginInput::Call(id, call))?;
        self.flush()?;

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
                ReceivedPluginCallMessage::Error(err) => {
                    return Err(err);
                }
                ReceivedPluginCallMessage::EngineCall(engine_call_id, engine_call) => {
                    let resp = handle_engine_call(engine_call, context)
                        .unwrap_or_else(EngineCallResponse::Error);
                    // Handle stream
                    let (resp, writer) = match resp {
                        EngineCallResponse::Error(error) => {
                            (EngineCallResponse::Error(error), None)
                        }
                        EngineCallResponse::Config(config) => {
                            (EngineCallResponse::Config(config), None)
                        }
                        EngineCallResponse::PipelineData(data) => {
                            match self.init_write_pipeline_data(data, context) {
                                Ok((header, writer)) => {
                                    (EngineCallResponse::PipelineData(header), Some(writer))
                                }
                                // just respond with the error if we fail to set it up
                                Err(err) => (EngineCallResponse::Error(err), None),
                            }
                        }
                    };
                    // Write the response, then the stream
                    self.write(PluginInput::EngineCallResponse(engine_call_id, resp))?;
                    self.flush()?;
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

    /// Get the command signatures from the plugin.
    pub(crate) fn get_signature(&self) -> Result<Vec<PluginSignature>, ShellError> {
        match self.plugin_call(PluginCall::Signature, &None)? {
            PluginCallResponse::Signature(sigs) => Ok(sigs),
            PluginCallResponse::Error(err) => Err(err.into()),
            _ => Err(ShellError::PluginFailedToDecode {
                msg: "Received unexpected response to plugin Signature call".into(),
            }),
        }
    }

    /// Run the plugin with the given call and execution context.
    pub(crate) fn run(
        &self,
        call: CallInfo<PipelineData>,
        context: Arc<impl PluginExecutionContext + 'static>,
    ) -> Result<PipelineData, ShellError> {
        let context = Some(Context(context));
        match self.plugin_call(PluginCall::Run(call), &context)? {
            PluginCallResponse::PipelineData(data) => Ok(data),
            PluginCallResponse::Error(err) => Err(err.into()),
            _ => Err(ShellError::PluginFailedToDecode {
                msg: "Received unexpected response to plugin Run call".into(),
            }),
        }
    }

    /// Collapse a custom value to its base value.
    pub(crate) fn collapse_custom_value(
        &self,
        data: PluginData,
        context: Arc<impl PluginExecutionContext + 'static>,
    ) -> Result<Value, ShellError> {
        let span = data.span;
        let context = Some(Context(context));
        match self.plugin_call(PluginCall::CollapseCustomValue(data), &context)? {
            PluginCallResponse::PipelineData(out_data) => Ok(out_data.into_value(span)),
            PluginCallResponse::Error(err) => Err(err.into()),
            _ => Err(ShellError::PluginFailedToDecode {
                msg: "Received unexpected response to plugin CollapseCustomValue call".into(),
            }),
        }
    }
}

impl Interface for PluginInterface {
    type Output = PluginInput;
    type Context = Option<Context>;

    fn write(&self, input: PluginInput) -> Result<(), ShellError> {
        log::trace!("to plugin: {:?}", input);
        self.state.writer.write_input(&input)
    }

    fn flush(&self) -> Result<(), ShellError> {
        self.state.writer.flush()
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
            let context = context
                .as_ref()
                .ok_or_else(|| ShellError::PluginFailedToEncode {
                    msg: "Can't handle PluginCustomValue without knowing the context".into(),
                })?;
            match val.as_any().downcast_ref::<PluginCustomValue>() {
                Some(plugin_data) if plugin_data.filename == context.filename() => {
                    Ok(Some(PluginData {
                        name: None, // plugin doesn't need it.
                        data: plugin_data.data.clone(),
                        span: value.span(),
                    }))
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
    let call_name = call.name();
    let require_context = || {
        context.as_ref().ok_or_else(|| ShellError::GenericError {
            error: "A plugin execution context is required for this engine call".into(),
            msg: format!(
                "attempted to call {} outside of a command invocation",
                call_name
            ),
            span: None,
            help: Some("this is probably a bug with the plugin".into()),
            inner: vec![],
        })
    };
    match call {
        EngineCall::GetConfig => {
            let context = require_context()?;
            let config = Box::new(context.get_config()?.clone());
            Ok(EngineCallResponse::Config(config))
        }
        EngineCall::EvalClosure {
            closure,
            positional,
            input,
            redirect_stdout,
            redirect_stderr,
        } => require_context()?
            .eval_closure(closure, positional, input, redirect_stdout, redirect_stderr)
            .map(EngineCallResponse::PipelineData),
    }
}
