mod evaluated_call;
mod plugin_custom_value;
mod plugin_data;

pub use evaluated_call::EvaluatedCall;
use nu_protocol::{
    engine::Closure, Config, PluginSignature, RawStream, ShellError, Span, Spanned, Value,
};
pub use plugin_custom_value::PluginCustomValue;
pub use plugin_data::PluginData;
use serde::{Deserialize, Serialize};

/// A sequential identifier for a stream
pub type StreamId = usize;

/// A sequential identifer for a [`PluginCall`]
pub type PluginCallId = usize;

/// A sequential identifier for an [`EngineCall`]
pub type EngineCallId = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CallInfo<D> {
    pub name: String,
    pub call: EvaluatedCall,
    pub input: D,
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
    /// Initiate [nu_protocol::PipelineData::ListStream].
    ///
    /// Items are sent via [StreamData]
    ListStream(ListStreamInfo),
    /// Initiate [nu_protocol::PipelineData::ExternalStream].
    ///
    /// Items are sent via [StreamData]
    ExternalStream(ExternalStreamInfo),
}

/// Additional information about list (value) streams
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ListStreamInfo {
    pub id: StreamId,
}

/// Additional information about external streams
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ExternalStreamInfo {
    pub span: Span,
    pub stdout: Option<RawStreamInfo>,
    pub stderr: Option<RawStreamInfo>,
    pub exit_code: Option<ListStreamInfo>,
    pub trim_end_newline: bool,
}

/// Additional information about raw (byte) streams
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct RawStreamInfo {
    pub id: StreamId,
    pub is_binary: bool,
    pub known_size: Option<u64>,
}

impl RawStreamInfo {
    pub(crate) fn new(id: StreamId, stream: &RawStream) -> Self {
        RawStreamInfo {
            id,
            is_binary: stream.is_binary,
            known_size: stream.known_size,
        }
    }
}

/// Initial message sent to the plugin
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PluginCall<D> {
    Signature,
    Run(CallInfo<D>),
    CollapseCustomValue(PluginData),
}

/// Any data sent to the plugin
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PluginInput {
    Call(PluginCallId, PluginCall<PipelineDataHeader>),
    Stream(StreamMessage),
    EngineCallResponse(EngineCallId, EngineCallResponse<PipelineDataHeader>),
}

impl TryFrom<PluginInput> for StreamMessage {
    type Error = PluginInput;

    fn try_from(msg: PluginInput) -> Result<StreamMessage, PluginInput> {
        match msg {
            PluginInput::Stream(stream_msg) => Ok(stream_msg),
            _ => Err(msg),
        }
    }
}

impl From<StreamMessage> for PluginInput {
    fn from(stream_msg: StreamMessage) -> PluginInput {
        PluginInput::Stream(stream_msg)
    }
}

/// A single item of stream data for a stream.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StreamData {
    List(Value),
    Raw(Result<Vec<u8>, ShellError>),
}

impl From<Value> for StreamData {
    fn from(value: Value) -> Self {
        StreamData::List(value)
    }
}

impl From<Result<Vec<u8>, ShellError>> for StreamData {
    fn from(value: Result<Vec<u8>, ShellError>) -> Self {
        StreamData::Raw(value)
    }
}

impl TryFrom<StreamData> for Value {
    type Error = ShellError;

    fn try_from(data: StreamData) -> Result<Value, ShellError> {
        match data {
            StreamData::List(value) => Ok(value),
            StreamData::Raw(_) => Err(ShellError::PluginFailedToDecode {
                msg: "expected list stream data, found raw data".into(),
            }),
        }
    }
}

impl TryFrom<StreamData> for Result<Vec<u8>, ShellError> {
    type Error = ShellError;

    fn try_from(data: StreamData) -> Result<Result<Vec<u8>, ShellError>, ShellError> {
        match data {
            StreamData::Raw(value) => Ok(value),
            StreamData::List(_) => Err(ShellError::PluginFailedToDecode {
                msg: "expected raw stream data, found list data".into(),
            }),
        }
    }
}

/// A stream control or data message.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StreamMessage {
    /// Append data to the stream. Sent by the stream producer.
    Data(StreamId, StreamData),
    /// End of stream. Sent by the stream producer.
    End(StreamId),
    /// Notify that the read end of the stream has closed, and further messages should not be
    /// sent. Sent by the stream consumer.
    Drop(StreamId),
    /// Acknowledge that a message has been consumed. This is used to implement flow control by
    /// the stream producer. Sent by the stream consumer.
    Ack(StreamId),
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
        ShellError::GenericError {
            error: error.label,
            msg: error.msg,
            span: error.span,
            help: None,
            inner: vec![],
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
                label: format!("Plugin failed to load: {msg}"),
                msg: "".into(),
                span: None,
            },
            ShellError::PluginFailedToEncode { msg } => LabeledError {
                label: format!("Plugin failed to encode: {msg}"),
                msg: "".into(),
                span: None,
            },
            ShellError::PluginFailedToDecode { msg } => LabeledError {
                label: format!("Plugin failed to decode: {msg}").into(),
                msg: "".into(),
                span: None,
            },
            ShellError::NushellFailed { msg } => LabeledError {
                label: format!("Nushell failed: {msg}"),
                msg: "This shouldn't happen. Please file an issue: https://github.com/nushell/nushell/issues".into(),
                span: None,
            },
            ShellError::NushellFailedSpanned { msg, label, span } => LabeledError {
                msg: label,
                label: msg,
                span: Some(span),
            },
            ShellError::NushellFailedHelp { msg, help } => LabeledError {
                label: format!("Nushell failed: {msg}"),
                msg: help,
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
pub enum PluginCallResponse<D> {
    Error(LabeledError),
    Signature(Vec<PluginSignature>),
    PipelineData(D),
}

impl PluginCallResponse<PipelineDataHeader> {
    /// Construct a plugin call response with a single value
    pub fn value(value: Value) -> PluginCallResponse<PipelineDataHeader> {
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
    CallResponse(PluginCallId, PluginCallResponse<PipelineDataHeader>),
    Stream(StreamMessage),
    EngineCall {
        context: PluginCallId,
        id: EngineCallId,
        call: EngineCall<PipelineDataHeader>,
    },
}

impl TryFrom<PluginOutput> for StreamMessage {
    type Error = PluginOutput;

    fn try_from(msg: PluginOutput) -> Result<StreamMessage, PluginOutput> {
        match msg {
            PluginOutput::Stream(stream_msg) => Ok(stream_msg),
            _ => Err(msg),
        }
    }
}

impl From<StreamMessage> for PluginOutput {
    fn from(stream_msg: StreamMessage) -> PluginOutput {
        PluginOutput::Stream(stream_msg)
    }
}

/// A remote call back to the engine during the plugin's execution.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EngineCall<D> {
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
        input: D,
        /// Whether to redirect stdout from external commands
        redirect_stdout: bool,
        /// Whether to redirect stderr from external commands
        redirect_stderr: bool,
    },
}

impl<D> EngineCall<D> {
    /// Get the name of the engine call so it can be embedded in things like error messages
    pub fn name(&self) -> &'static str {
        match self {
            EngineCall::GetConfig => "GetConfig",
            EngineCall::EvalClosure { .. } => "EvalClosure",
        }
    }
}

/// The response to an [EngineCall].
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EngineCallResponse<D> {
    Error(ShellError),
    PipelineData(D),
    Config(Box<Config>),
}
