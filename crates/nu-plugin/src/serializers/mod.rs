use crate::plugin::{PluginEncoderName, PluginEncoder};
use nu_protocol::ShellError;

pub mod json;
pub mod msgpack;

#[cfg(test)]
mod tests;

#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum EncodingType {
    Json(json::JsonSerializer),
    MsgPack(msgpack::MsgPackSerializer),
}

impl EncodingType {
    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"json" => Some(Self::Json(json::JsonSerializer {})),
            b"msgpack" => Some(Self::MsgPack(msgpack::MsgPackSerializer {})),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Json(_) => "json",
            Self::MsgPack(_) => "msgpack",
        }
    }
}

impl PluginEncoderName for EncodingType {
    fn name(&self) -> &str {
        self.to_str()
    }
}

impl<T> PluginEncoder<T> for EncodingType
where
    json::JsonSerializer: PluginEncoder<T>,
    msgpack::MsgPackSerializer: PluginEncoder<T>,
{
    fn encode(
        &self,
        data: &T,
        writer: &mut impl std::io::Write,
    ) -> Result<(), ShellError> {
        match self {
            EncodingType::Json(encoder) => encoder.encode(data, writer),
            EncodingType::MsgPack(encoder) => encoder.encode(data, writer),
        }
    }

    fn decode(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<T>, ShellError> {
        match self {
            EncodingType::Json(encoder) => encoder.decode(reader),
            EncodingType::MsgPack(encoder) => encoder.decode(reader),
        }
    }
}
