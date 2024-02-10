use nu_protocol::Span;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PluginData {
    /// The string representation of the custom value. Required when sending to the Engine.
    pub name: Option<String>,
    /// The serialized data of the custom value. The representation is specific to the plugin and
    /// can only be deserialized there.
    pub data: Vec<u8>,
    pub span: Span,
}
