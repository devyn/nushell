use std::{path::PathBuf, sync::Arc};

use nu_protocol::{CustomValue, ShellError, Value};
use serde::Serialize;

use crate::plugin::{create_command, make_plugin_interface, PluginExecutionNonCommandContext};

use super::PluginData;

/// An opaque container for a custom value that is handled fully by a plugin
///
/// This is constructed by the main nushell engine when it receives [`PluginResponse::PluginData`]
/// it stores that data as well as metadata related to the plugin to be able to call the plugin
/// later.
/// Since the data in it is opaque to the engine, there are only two final destinations for it:
/// either it will be sent back to the plugin that generated it across a pipeline, or it will be
/// sent to the plugin with a request to collapse it into a base value
#[derive(Clone, Debug, Serialize)]
pub struct PluginCustomValue {
    /// The name of the custom value as defined by the plugin
    pub name: String,
    pub data: Vec<u8>,
    pub filename: PathBuf,

    // PluginCustomValue must implement Serialize because all CustomValues must implement Serialize
    // However, the main place where values are serialized and deserialized is when they are being
    // sent between plugins and nushell's main engine. PluginCustomValue is never meant to be sent
    // between that boundary
    #[serde(skip)]
    pub shell: Option<PathBuf>,
    #[serde(skip)]
    pub source: String,
}

impl CustomValue for PluginCustomValue {
    fn clone_value(&self, span: nu_protocol::Span) -> nu_protocol::Value {
        Value::custom_value(Box::new(self.clone()), span)
    }

    fn value_string(&self) -> String {
        self.name.clone()
    }

    fn to_base_value(
        &self,
        span: nu_protocol::Span,
    ) -> Result<nu_protocol::Value, nu_protocol::ShellError> {
        let mut plugin_cmd = create_command(&self.filename, self.shell.as_deref());

        let wrap_err = |err: String| ShellError::GenericError {
            error: format!(
                "Unable to spawn plugin for `{}` to get base value",
                self.source
            ),
            msg: err,
            span: Some(span),
            help: None,
            inner: vec![],
        };

        let child = plugin_cmd
            .spawn()
            .map_err(|err| wrap_err(err.to_string()))?;

        let plugin_data = PluginData {
            name: None,
            data: self.data.clone(),
            span,
        };

        let context = Arc::new(PluginExecutionNonCommandContext::new(
            self.filename.clone(),
            self.shell.clone(),
            self.source.clone(),
            span,
        ));

        let interface = make_plugin_interface(child).map_err(|err| wrap_err(err.to_string()))?;

        interface
            .collapse_custom_value(plugin_data, context)
            .map_err(|err| wrap_err(err.to_string()))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn typetag_name(&self) -> &'static str {
        "PluginCustomValue"
    }

    fn typetag_deserialize(&self) {
        unimplemented!("typetag_deserialize")
    }
}
