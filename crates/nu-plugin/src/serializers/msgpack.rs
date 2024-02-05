use std::io::ErrorKind;

use crate::{plugin::PluginEncoder, protocol::{PluginOutput, PluginInput}};
use nu_protocol::ShellError;
use serde::Deserialize;

/// A `PluginEncoder` that enables the plugin to communicate with Nushell with MsgPack
/// serialized data.
///
/// Each message is written as a MessagePack object. There is no message envelope or separator.
#[derive(Clone, Debug)]
pub struct MsgPackSerializer;

impl PluginEncoder for MsgPackSerializer {
    fn name(&self) -> &str {
        "msgpack"
    }

    fn encode_input(
        &self,
        plugin_input: &PluginInput,
        writer: &mut impl std::io::Write,
    ) -> Result<(), nu_protocol::ShellError> {
        rmp_serde::encode::write(writer, plugin_input).map_err(|err| {
            ShellError::PluginFailedToEncode {
                msg: err.to_string(),
            }
        })
    }

    fn decode_input(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginInput>, ShellError> {
        let mut de = rmp_serde::Deserializer::new(reader);
        PluginInput::deserialize(&mut de).map(Some).or_else(|err| {
            if rmp_error_is_eof(&err) {
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
        rmp_serde::encode::write(writer, plugin_output).map_err(|err| {
            ShellError::PluginFailedToEncode {
                msg: err.to_string(),
            }
        })
    }

    fn decode_output(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginOutput>, ShellError> {
        let mut de = rmp_serde::Deserializer::new(reader);
        PluginOutput::deserialize(&mut de).map(Some).or_else(|err| {
            if rmp_error_is_eof(&err) {
                Ok(None)
            } else {
                Err(ShellError::PluginFailedToDecode {
                    msg: err.to_string(),
                })
            }
        })
    }
}

fn rmp_error_is_eof(err: &rmp_serde::decode::Error) -> bool {
    match err {
        rmp_serde::decode::Error::InvalidMarkerRead(err)
            if matches!(err.kind(), ErrorKind::UnexpectedEof) => true,
        _ => false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    crate::serializers::tests::generate_tests!(MsgPackSerializer {});
}
