use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

use nu_engine::eval_block_with_early_return;
use nu_protocol::{
    ast::Call,
    engine::{EngineState, Stack, Closure},
    Span, Value, PipelineData, ShellError, Config, Spanned,
};

/// Object safe trait for abstracting operations required of the plugin context.
pub(crate) trait PluginExecutionContext: Send + Sync {
    /// The plugin's filename
    fn filename(&self) -> &Path;
    /// The shell used to execute the plugin
    fn shell(&self) -> Option<&Path>;
    /// The [Span] for the command execution (`call.head`)
    fn command_span(&self) -> Span;
    /// The name of the command being executed
    fn command_name(&self) -> &str;
    /// The interrupt signal, if present
    fn ctrlc(&self) -> Option<&Arc<AtomicBool>>;
    /// Get engine configuration
    fn get_config(&self) -> &Config;
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

/// The execution context of a plugin.
pub(crate) struct PluginExecutionNushellContext {
    filename: PathBuf,
    shell: Option<PathBuf>,
    engine_state: EngineState,
    stack: Stack,
    call: Call,
}

impl PluginExecutionNushellContext {
    pub fn new(
        filename: impl Into<PathBuf>,
        shell: Option<impl Into<PathBuf>>,
        engine_state: &EngineState,
        stack: &Stack,
        call: &Call,
    ) -> PluginExecutionNushellContext {
        PluginExecutionNushellContext {
            filename: filename.into(),
            shell: shell.map(Into::into),
            engine_state: engine_state.clone(),
            stack: stack.clone(),
            call: call.clone(),
        }
    }
}

impl PluginExecutionContext for PluginExecutionNushellContext {
    fn filename(&self) -> &Path {
        &self.filename
    }

    fn shell(&self) -> Option<&Path> {
        self.shell.as_deref()
    }

    fn command_span(&self) -> Span {
        self.call.head
    }

    fn command_name(&self) -> &str {
        self.engine_state.get_decl(self.call.decl_id).name()
    }

    fn ctrlc(&self) -> Option<&Arc<AtomicBool>> {
        self.engine_state.ctrlc.as_ref()
    }

    fn get_config(&self) -> &Config {
        self.engine_state.get_config()
    }

    fn eval_closure(
        &self,
        closure: Spanned<Closure>,
        positional: Vec<Value>,
        input: PipelineData,
        redirect_stdout: bool,
        redirect_stderr: bool,
    ) -> Result<PipelineData, ShellError> {
        let block = self.engine_state.try_get_block(closure.item.block_id).ok_or_else(|| {
            ShellError::GenericError {
                error: "Plugin misbehaving".into(),
                msg: format!("Tried to evaluate unknown block id: {}", closure.item.block_id),
                span: Some(closure.span),
                help: None,
                inner: vec![],
            }
        })?;

        let mut stack = self.stack.captures_to_stack(closure.item.captures);

        // Set up the positional arguments
        for (idx, value) in positional.into_iter().enumerate() {
            if let Some(arg) = block.signature.get_positional(idx) {
                stack.add_var(arg.var_id.expect("closure arg missing var_id"), value);
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
