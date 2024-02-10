use std::path::PathBuf;

use nu_protocol::{CustomValue, ShellError, Value};
use serde::Serialize;

use crate::plugin::{create_command, make_plugin_interface};

use super::{PipelineDataHeader, PluginCall, PluginCallResponse, PluginData};

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

        let mut child = plugin_cmd.spawn().map_err(|err| ShellError::GenericError {
            error: format!(
                "Unable to spawn plugin for {} to get base value",
                self.source
            ),
            msg: format!("{err}"),
            span: Some(span),
            help: None,
            inner: vec![],
        })?;

        let plugin_call = PluginCall::CollapseCustomValue(PluginData {
            name: None,
            data: self.data.clone(),
            span,
        });

        let interface = make_plugin_interface(&mut child, None)?;
        let interface_clone = interface.clone();

        // Write the call on another thread to avoid blocking
        std::thread::spawn(move || interface_clone.write_call(plugin_call));

        let response = interface
            .read_call_response()
            .map_err(|err| ShellError::GenericError {
                error: format!(
                    "Unable to decode call for {} to get base value",
                    self.source
                ),
                msg: format!("{err}"),
                span: Some(span),
                help: None,
                inner: vec![],
            });

        drop(interface);

        let value = match response {
            Ok(PluginCallResponse::PipelineData(data)) => match data {
                PipelineDataHeader::Value(value) => Ok(value),
                PipelineDataHeader::PluginData(..) => Err(ShellError::GenericError {
                    error: "Plugin misbehaving".into(),
                    msg: "Plugin returned custom data as a response to a collapse call".into(),
                    span: Some(span),
                    help: None,
                    inner: vec![],
                }),
                PipelineDataHeader::Empty
                | PipelineDataHeader::ListStream(..)
                | PipelineDataHeader::ExternalStream(..) => Err(ShellError::GenericError {
                    error: "Plugin misbehaving".into(),
                    msg: "Plugin returned stream as a response to a collapse call".into(),
                    span: Some(span),
                    help: None,
                    inner: vec![],
                }),
            },
            Ok(PluginCallResponse::Error(err)) => Err(err.into()),
            Ok(PluginCallResponse::Signature(..)) => Err(ShellError::GenericError {
                error: "Plugin missing value".into(),
                msg: "Received a signature from plugin instead of value".into(),
                span: Some(span),
                help: None,
                inner: vec![],
            }),
            Err(err) => Err(err),
        };

        // We need to call .wait() on the child, or we'll risk summoning the zombie horde
        let _ = child.wait();

        value
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
