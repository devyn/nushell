use crate::{plugin::PluginEncoder, protocol::{PluginInput, PluginOutput}};
use nu_protocol::ShellError;
use serde::Deserialize;

/// A `PluginEncoder` that enables the plugin to communicate with Nushell with JSON
/// serialized data.
///
/// Each message in the stream is followed by a newline when serializing, but is not required for
/// deserialization. The output is not pretty printed and each object does not contain newlines.
/// If it is more convenient, a plugin may choose to separate messages by newline.
#[derive(Clone, Debug)]
pub struct JsonSerializer;

impl PluginEncoder for JsonSerializer {
    fn name(&self) -> &str {
        "json"
    }

    fn encode_input(
        &self,
        plugin_input: &PluginInput,
        writer: &mut impl std::io::Write,
    ) -> Result<(), nu_protocol::ShellError> {
        serde_json::to_writer(&mut *writer, plugin_input).map_err(|err| ShellError::PluginFailedToEncode {
            msg: err.to_string(),
        })?;
        writer.write_all(b"\n").map_err(|err| ShellError::PluginFailedToEncode {
            msg: err.to_string()
        })
    }

    fn decode_input(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginInput>, nu_protocol::ShellError> {
        let mut de = serde_json::Deserializer::from_reader(reader);
        PluginInput::deserialize(&mut de).map(Some).or_else(|err| {
            if err.is_eof() {
                Ok(None)
            } else {
                Err(ShellError::PluginFailedToDecode {
                    msg: err.to_string(),
                })
            }
        })
    }

    fn encode_output(
        &self,
        plugin_output: &PluginOutput,
        writer: &mut impl std::io::Write,
    ) -> Result<(), ShellError> {
        serde_json::to_writer(&mut *writer, plugin_output).map_err(|err| {
            ShellError::PluginFailedToEncode {
                msg: err.to_string(),
            }
        })?;
        writer.write_all(b"\n").map_err(|err| ShellError::PluginFailedToEncode {
            msg: err.to_string()
        })
    }

    fn decode_output(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginOutput>, ShellError> {
        let mut de = serde_json::Deserializer::from_reader(reader);
        PluginOutput::deserialize(&mut de).map(Some).or_else(|err| {
            if err.is_eof() {
                Ok(None)
            } else {
                Err(ShellError::PluginFailedToDecode {
                    msg: err.to_string(),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::serializers::tests::generate_tests!(JsonSerializer {});
}
