use std::sync::{atomic::AtomicBool, Arc};

use nu_engine::eval_block_with_early_return;
use nu_protocol::{
    ast::Call,
    engine::{Closure, EngineState, Stack},
    Config, PipelineData, ShellError, Span, Spanned, Value,
};

/// Object safe trait for abstracting operations required of the plugin context.
pub(crate) trait PluginExecutionContext: Send + Sync {
    /// The [Span] for the command execution (`call.head`)
    fn command_span(&self) -> Span;
    /// The name of the command being executed
    fn command_name(&self) -> &str;
    /// The interrupt signal, if present
    fn ctrlc(&self) -> Option<&Arc<AtomicBool>>;
    /// Get engine configuration
    fn get_config(&self) -> Result<Config, ShellError>;
    /// Evaluate a closure passed to the plugin
    fn eval_closure(
        &self,
        closure: Spanned<Closure>,
        positional: Vec<Value>,
        input: PipelineData,
        redirect_stdout: bool,
        redirect_stderr: bool,
    ) -> Result<PipelineData, ShellError>;
}

/// The execution context of a plugin command.
pub(crate) struct PluginExecutionCommandContext {
    engine_state: EngineState,
    stack: Stack,
    call: Call,
}

impl PluginExecutionCommandContext {
    pub fn new(
        engine_state: &EngineState,
        stack: &Stack,
        call: &Call,
    ) -> PluginExecutionCommandContext {
        PluginExecutionCommandContext {
            engine_state: engine_state.clone(),
            stack: stack.clone(),
            call: call.clone(),
        }
    }
}

impl PluginExecutionContext for PluginExecutionCommandContext {
    fn command_span(&self) -> Span {
        self.call.head
    }

    fn command_name(&self) -> &str {
        self.engine_state.get_decl(self.call.decl_id).name()
    }

    fn ctrlc(&self) -> Option<&Arc<AtomicBool>> {
        self.engine_state.ctrlc.as_ref()
    }

    fn get_config(&self) -> Result<Config, ShellError> {
        Ok(nu_engine::get_config(&self.engine_state, &self.stack))
    }

    fn eval_closure(
        &self,
        closure: Spanned<Closure>,
        positional: Vec<Value>,
        input: PipelineData,
        redirect_stdout: bool,
        redirect_stderr: bool,
    ) -> Result<PipelineData, ShellError> {
        let block = self
            .engine_state
            .try_get_block(closure.item.block_id)
            .ok_or_else(|| ShellError::GenericError {
                error: "Plugin misbehaving".into(),
                msg: format!(
                    "Tried to evaluate unknown block id: {}",
                    closure.item.block_id
                ),
                span: Some(closure.span),
                help: None,
                inner: vec![],
            })?;

        let mut stack = self.stack.captures_to_stack(closure.item.captures);

        // Set up the positional arguments
        for (idx, value) in positional.into_iter().enumerate() {
            if let Some(arg) = block.signature.get_positional(idx) {
                if let Some(var_id) = arg.var_id {
                    stack.add_var(var_id, value);
                } else {
                    return Err(ShellError::NushellFailedSpanned {
                        msg: "Error while evaluating closure from plugin".into(),
                        label: "closure argument missing var_id".into(),
                        span: closure.span,
                    });
                }
            }
        }

        eval_block_with_early_return(
            &self.engine_state,
            &mut stack,
            block,
            input,
            redirect_stdout,
            redirect_stderr,
        )
    }
}
