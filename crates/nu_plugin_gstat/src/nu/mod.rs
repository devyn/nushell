use crate::GStat;
use nu_plugin::{
    EngineInterface, EvaluatedCall, LabeledError, Plugin, PluginCommand, SimplePluginCommand,
};
use nu_protocol::{Category, PluginSignature, Spanned, SyntaxShape, Value, NuString};

pub struct GStatPlugin;

impl Plugin for GStatPlugin {
    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(GStat)]
    }
}

impl SimplePluginCommand for GStat {
    type Plugin = GStatPlugin;

    fn signature(&self) -> PluginSignature {
        PluginSignature::build("gstat")
            .usage("Get the git status of a repo")
            .optional("path", SyntaxShape::Filepath, "path to repo")
            .category(Category::Custom("prompt".into()))
    }

    fn run(
        &self,
        _plugin: &GStatPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let repo_path: Option<Spanned<NuString>> = call.opt(0)?;
        // eprintln!("input value: {:#?}", &input);
        let current_dir = engine.get_current_dir()?;
        self.gstat(input, &current_dir, repo_path, call.head)
    }
}
