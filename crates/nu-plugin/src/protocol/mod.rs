mod evaluated_call;
mod plugin_custom_value;
mod plugin_data;

pub use evaluated_call::EvaluatedCall;
use nu_protocol::{PluginSignature, RawStream, ShellError, Span, Value, engine::Closure, Config, Spanned};
pub use plugin_custom_value::PluginCustomValue;
pub use plugin_data::PluginData;
use serde::{Deserialize, Serialize};

/// A sequential identifier for a stream
pub type StreamId = usize;

/// A sequential identifier for an [EngineCall]
pub type EngineCallId = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CallInfo {
    pub name: String,
    pub call: EvaluatedCall,
    pub input: PipelineDataHeader,
    pub config: Option<Value>,
}

/// The initial (and perhaps only) part of any [nu_protocol::PipelineData] sent over the wire.
///
/// This may contain a single value, or may initiate a stream with a [StreamId].
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum PipelineDataHeader {
    /// No input
    Empty,
    /// A single value
    Value(Value),
    /// Represents a [nu_protocol::CustomValue] on the plugin side, which should be encapsulated in
    /// [PluginCustomValue] on the engine side.
    PluginData(PluginData),
    /// Initiate [nu_protocol::PipelineData::ListStream] with the given [StreamId].
    ///
    /// Items are sent via [StreamData]
    ListStream(StreamId),
    /// Initiate [nu_protocol::PipelineData::ExternalStream] with the given [StreamId].
    ///
    /// Items are sent via [StreamData]
    ExternalStream(StreamId, ExternalStreamInfo),
}

/// Additional information about external streams
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ExternalStreamInfo {
    pub span: Span,
    pub stdout: Option<RawStreamInfo>,
    pub stderr: Option<RawStreamInfo>,
    pub has_exit_code: bool,
    pub trim_end_newline: bool,
}

/// Additional information about raw streams
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct RawStreamInfo {
    pub is_binary: bool,
    pub known_size: Option<u64>,
}

impl From<&RawStream> for RawStreamInfo {
    fn from(stream: &RawStream) -> Self {
        RawStreamInfo {
            is_binary: stream.is_binary,
            known_size: stream.known_size,
        }
    }
}

/// Initial message sent to the plugin
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PluginCall {
    Signature,
    Run(CallInfo),
    CollapseCustomValue(PluginData),
}

/// Any data sent to the plugin
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PluginInput {
    Call(PluginCall),
    StreamData(StreamId, StreamData),
    EngineCallResponse(EngineCallId, EngineCallResponse),
}

/// A single item of stream data for a stream.
///
/// A `None` value ends the stream. An `Error` ends all streams, and the error should be propagated.
///
/// Note: exported for internal use, not public.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[doc(hidden)]
pub enum StreamData {
    List(Option<Value>),
    ExternalStdout(Option<Result<Vec<u8>, ShellError>>),
    ExternalStderr(Option<Result<Vec<u8>, ShellError>>),
    ExternalExitCode(Option<Value>),
}

/// An error message with debugging information that can be passed to Nushell from the plugin
///
/// The `LabeledError` struct is a structured error message that can be returned from
/// a [Plugin](crate::Plugin)'s [`run`](crate::Plugin::run()) method. It contains
/// the error message along with optional [Span] data to support highlighting in the
/// shell.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct LabeledError {
    /// The name of the error
    pub label: String,
    /// A detailed error description
    pub msg: String,
    /// The [Span] in which the error occurred
    pub span: Option<Span>,
}

impl From<LabeledError> for ShellError {
    fn from(error: LabeledError) -> Self {
        match error.span {
            Some(span) => ShellError::GenericError {
                error: error.label,
                msg: error.msg,
                span: Some(span),
                help: None,
                inner: vec![],
            },
            None => ShellError::GenericError {
                error: error.label,
                msg: "".into(),
                span: None,
                help: Some(error.msg),
                inner: vec![],
            },
        }
    }
}

impl From<ShellError> for LabeledError {
    fn from(error: ShellError) -> Self {
        match error {
            ShellError::GenericError {
                error: label,
                msg,
                span,
                ..
            } => LabeledError { label, msg, span },
            ShellError::CantConvert {
                to_type: expected,
                from_type: input,
                span,
                help: _help,
            } => LabeledError {
                label: format!("Can't convert to {expected}"),
                msg: format!("can't convert from {input} to {expected}"),
                span: Some(span),
            },
            ShellError::DidYouMean { suggestion, span } => LabeledError {
                label: "Name not found".into(),
                msg: format!("did you mean '{suggestion}'?"),
                span: Some(span),
            },
            ShellError::PluginFailedToLoad { msg } => LabeledError {
                label: "Plugin failed to load".into(),
                msg,
                span: None,
            },
            ShellError::PluginFailedToEncode { msg } => LabeledError {
                label: "Plugin failed to encode".into(),
                msg,
                span: None,
            },
            ShellError::PluginFailedToDecode { msg } => LabeledError {
                label: "Plugin failed to decode".into(),
                msg,
                span: None,
            },
            err => LabeledError {
                label: format!("Error - Add to LabeledError From<ShellError>: {err:?}"),
                msg: err.to_string(),
                span: None,
            },
        }
    }
}

/// Response to a [PluginCall]
///
/// Note: exported for internal use, not public.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[doc(hidden)]
pub enum PluginCallResponse {
    Error(LabeledError),
    Signature(Vec<PluginSignature>),
    PipelineData(PipelineDataHeader),
}

impl PluginCallResponse {
    /// Construct a plugin call response with a single value
    pub fn value(value: Value) -> PluginCallResponse {
        if value.is_nothing() {
            PluginCallResponse::PipelineData(PipelineDataHeader::Empty)
        } else {
            PluginCallResponse::PipelineData(PipelineDataHeader::Value(value))
        }
    }
}

/// Information received from the plugin
///
/// Note: exported for internal use, not public.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[doc(hidden)]
pub enum PluginOutput {
    CallResponse(PluginCallResponse),
    StreamData(StreamId, StreamData),
    EngineCall(EngineCallId, EngineCall),
}

/// A remote call back to the engine during the plugin's execution.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EngineCall {
    /// Get the full engine configuration
    GetConfig,
    /// Evaluate a closure with stream input/output
    EvalClosure {
        /// The closure to call.
        ///
        /// This may come from a [`Value::Closure`] passed in as an argument to the plugin.
        closure: Spanned<Closure>,
        /// Positional arguments to add to the closure call
        positional: Vec<Value>,
        /// Input to the closure
        input: PipelineDataHeader,
        /// Whether to redirect stdout from external commands
        redirect_stdout: bool,
        /// Whether to redirect stderr from external commands
        redirect_stderr: bool,
    },
}

/// The response to an [EngineCall].
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EngineCallResponse {
    Error(ShellError),
    PipelineData(PipelineDataHeader),
    Config(Box<Config>),
}
