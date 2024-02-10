use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

use nu_protocol::{
    ast::Call,
    engine::{EngineState, Stack},
    Span,
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
}

/// The execution context of a plugin.
#[derive(Debug)]
pub(crate) struct PluginExecutionNushellContext {
    filename: PathBuf,
    shell: Option<PathBuf>,
    command_span: Span,
    command_name: String,
    ctrlc: Option<Arc<AtomicBool>>,
    // If more operations are required of the context, fields can be added here.
    //
    // It may be required to insert the entire EngineState/Call/Stack in here to support
    // future features and that's okay
}

impl PluginExecutionNushellContext {
    pub fn new(
        filename: impl Into<PathBuf>,
        shell: Option<impl Into<PathBuf>>,
        engine_state: &EngineState,
        _stack: &Stack,
        call: &Call,
    ) -> PluginExecutionNushellContext {
        PluginExecutionNushellContext {
            filename: filename.into(),
            shell: shell.map(Into::into),
            command_span: call.head,
            command_name: engine_state.get_decl(call.decl_id).name().to_owned(),
            ctrlc: engine_state.ctrlc.clone(),
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
        self.command_span
    }

    fn command_name(&self) -> &str {
        &self.command_name
    }

    fn ctrlc(&self) -> Option<&Arc<AtomicBool>> {
        self.ctrlc.as_ref()
    }
}
