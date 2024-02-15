mod declaration;
pub use declaration::PluginDeclaration;
use nu_engine::documentation::get_flags_section;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::plugin::interface::{EngineInterfaceManager, ReceivedPluginCall};
use crate::protocol::{
    CallInfo, LabeledError, PluginInput, PluginOutput,
};
use crate::EncodingType;
use std::env;
use std::fmt::Write;
use std::io::{BufReader, ErrorKind, Read, Write as WriteTrait};
use std::path::Path;
use std::process::{Child, ChildStdout, Command as CommandSys, Stdio};

use nu_protocol::{PipelineData, PluginSignature, ShellError, Value, CustomValue};

mod interface;
pub use interface::EngineInterface;
pub(crate) use interface::PluginInterface;

mod context;
pub(crate) use context::{PluginExecutionCommandContext, PluginExecutionNonCommandContext};

use self::interface::{PluginInterfaceManager, InterfaceManager};

use super::EvaluatedCall;

pub(crate) const OUTPUT_BUFFER_SIZE: usize = 8192;

/// Encoding scheme that defines a plugin's communication protocol with Nu
///
/// Note: exported for internal use, but not public.
#[doc(hidden)]
pub trait PluginEncoder: Clone + Send + Sync {
    /// The name of the encoder (e.g., `json`)
    fn name(&self) -> &str;

    /// Serialize a `PluginInput` in the `PluginEncoder`s format
    ///
    /// Returns [ShellError::IOError] if there was a problem writing, or
    /// [ShellError::PluginFailedToEncode] for a serialization error.
    fn encode_input(
        &self,
        plugin_input: &PluginInput,
        writer: &mut impl std::io::Write,
    ) -> Result<(), ShellError>;

    /// Deserialize a `PluginInput` from the `PluginEncoder`s format
    ///
    /// Returns `None` if there is no more input to receive, in which case the plugin should exit.
    ///
    /// Returns [ShellError::IOError] if there was a problem reading, or
    /// [ShellError::PluginFailedToDecode] for a deserialization error.
    fn decode_input(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginInput>, ShellError>;

    /// Serialize a `PluginOutput` in this `PluginEncoder`'s format
    ///
    /// Returns [ShellError::IOError] if there was a problem writing, or
    /// [ShellError::PluginFailedToEncode] for a serialization error.
    fn encode_output(
        &self,
        plugin_output: &PluginOutput,
        writer: &mut impl std::io::Write,
    ) -> Result<(), ShellError>;

    /// Deserialize a `PluginOutput` from the `PluginEncoder`'s format
    ///
    /// Returns `None` if there is no more output to receive.
    ///
    /// Returns [ShellError::IOError] if there was a problem reading, or
    /// [ShellError::PluginFailedToDecode] for a deserialization error.
    fn decode_output(
        &self,
        reader: &mut impl std::io::BufRead,
    ) -> Result<Option<PluginOutput>, ShellError>;
}

pub(crate) fn create_command(path: &Path, shell: Option<&Path>) -> CommandSys {
    log::trace!("Starting plugin: {path:?}, shell = {shell:?}");

    let mut process = match (path.extension(), shell) {
        (_, Some(shell)) => {
            let mut process = std::process::Command::new(shell);
            process.arg(path);

            process
        }
        (Some(extension), None) => {
            let (shell, separator) = match extension.to_str() {
                Some("cmd") | Some("bat") => (Some("cmd"), Some("/c")),
                Some("sh") => (Some("sh"), Some("-c")),
                Some("py") => (Some("python"), None),
                _ => (None, None),
            };

            match (shell, separator) {
                (Some(shell), Some(separator)) => {
                    let mut process = std::process::Command::new(shell);
                    process.arg(separator);
                    process.arg(path);

                    process
                }
                (Some(shell), None) => {
                    let mut process = std::process::Command::new(shell);
                    process.arg(path);

                    process
                }
                _ => std::process::Command::new(path),
            }
        }
        (None, None) => std::process::Command::new(path),
    };

    // Both stdout and stdin are piped so we can receive information from the plugin
    process.stdout(Stdio::piped()).stdin(Stdio::piped());

    process
}

pub(crate) fn make_plugin_interface(mut child: Child) -> Result<PluginInterface, ShellError> {
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| ShellError::PluginFailedToLoad {
            msg: "plugin missing stdin writer".into(),
        })?;

    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| ShellError::PluginFailedToLoad {
            msg: "Plugin missing stdout writer".into(),
        })?;

    let encoder = get_plugin_encoding(&mut stdout)?;

    let reader = BufReader::with_capacity(OUTPUT_BUFFER_SIZE, stdout);

    let mut manager = PluginInterfaceManager::new((Mutex::new(stdin), encoder.clone()));
    let interface = manager.get_interface();

    // Spawn the reader on a new thread
    std::thread::spawn(move || {
        if let Err(err) = manager.consume_all((reader, encoder)) {
            log::warn!("Error in PluginInterfaceManager: {err}");
        }
        // If the loop has ended, drop the manager so everyone disconnects and then wait for the
        // child to exit
        drop(manager);
        let _ = child.wait();
    });

    Ok(interface)
}

#[doc(hidden)] // Note: not for plugin authors / only used in nu-parser
pub fn get_signature(
    path: &Path,
    shell: Option<&Path>,
    current_envs: &HashMap<String, String>,
) -> Result<Vec<PluginSignature>, ShellError> {
    let mut plugin_cmd = create_command(path, shell);
    let program_name = plugin_cmd.get_program().to_os_string().into_string();

    plugin_cmd.envs(current_envs);
    let child = plugin_cmd.spawn().map_err(|err| {
        let error_msg = match err.kind() {
            ErrorKind::NotFound => match program_name {
                Ok(prog_name) => {
                    format!("Can't find {prog_name}, please make sure that {prog_name} is in PATH.")
                }
                _ => {
                    format!("Error spawning child process: {err}")
                }
            },
            _ => {
                format!("Error spawning child process: {err}")
            }
        };

        ShellError::PluginFailedToLoad { msg: error_msg }
    })?;

    // Communicate with the plugin to get the signature
    make_plugin_interface(child)?.get_signature()
}

/// The basic API for a Nushell plugin
///
/// This is the trait that Nushell plugins must implement. The methods defined on
/// `Plugin` are invoked by [serve_plugin] during plugin registration and execution.
///
/// If large amounts of data are expected to need to be received or produced, it may be more
/// appropriate to implement [StreamingPlugin] instead.
///
/// # Examples
/// Basic usage:
/// ```
/// # use nu_plugin::*;
/// # use nu_protocol::{PluginSignature, Type, Value};
/// struct HelloPlugin;
///
/// impl Plugin for HelloPlugin {
///     fn signature(&self) -> Vec<PluginSignature> {
///         let sig = PluginSignature::build("hello")
///             .input_output_type(Type::Nothing, Type::String);
///
///         vec![sig]
///     }
///
///     fn run(
///         &mut self,
///         name: &str,
///         config: &Option<Value>,
///         call: &EvaluatedCall,
///         input: &Value,
///     ) -> Result<Value, LabeledError> {
///         Ok(Value::string("Hello, World!".to_owned(), call.head))
///     }
/// }
/// ```
pub trait Plugin {
    /// The signature of the plugin
    ///
    /// This method returns the [PluginSignature]s that describe the capabilities
    /// of this plugin. Since a single plugin executable can support multiple invocation
    /// patterns we return a `Vec` of signatures.
    fn signature(&self) -> Vec<PluginSignature>;

    /// Perform the actual behavior of the plugin
    ///
    /// The behavior of the plugin is defined by the implementation of this method.
    /// When Nushell invoked the plugin [serve_plugin] will call this method and
    /// print the serialized returned value or error to stdout, which Nushell will
    /// interpret.
    ///
    /// The `name` is only relevant for plugins that implement multiple commands as the
    /// invoked command will be passed in via this argument. The `call` contains
    /// metadata describing how the plugin was invoked and `input` contains the structured
    /// data passed to the command implemented by this [Plugin].
    ///
    /// This variant does not support streaming. Consider implementing [StreamingPlugin] instead
    /// if streaming is desired.
    fn run(
        &mut self,
        name: &str,
        config: &Option<Value>,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError>;
}

/// The streaming API for a Nushell plugin
///
/// This is a more low-level version of the [Plugin] trait that supports operating on streams of
/// data. If you don't need to operate on streams, consider using that trait instead.
///
/// The methods defined on `StreamingPlugin` are invoked by [serve_plugin] during plugin
/// registration and execution.
///
/// # Examples
/// Basic usage:
/// ```
/// # use nu_plugin::*;
/// # use nu_protocol::{PluginSignature, PipelineData, Type, Value};
/// struct LowercasePlugin;
///
/// impl StreamingPlugin for LowercasePlugin {
///     fn signature(&self) -> Vec<PluginSignature> {
///         let sig = PluginSignature::build("lowercase")
///             .usage("Convert each string in a stream to lowercase")
///             .input_output_type(Type::List(Type::String.into()), Type::List(Type::String.into()));
///
///         vec![sig]
///     }
///
///     fn run(
///         &mut self,
///         name: &str,
///         config: &Option<Value>,
///         engine: &EngineInterface,
///         call: &EvaluatedCall,
///         input: PipelineData,
///     ) -> Result<PipelineData, LabeledError> {
///         let span = call.head;
///         Ok(input.map(move |value| {
///             value.as_string()
///                 .map(|string| Value::string(string.to_lowercase(), span))
///                 // Errors in a stream should be returned as values.
///                 .unwrap_or_else(|err| Value::error(err, span))
///         }, None)?)
///     }
/// }
/// ```
pub trait StreamingPlugin {
    /// The signature of the plugin
    ///
    /// This method returns the [PluginSignature]s that describe the capabilities
    /// of this plugin. Since a single plugin executable can support multiple invocation
    /// patterns we return a `Vec` of signatures.
    fn signature(&self) -> Vec<PluginSignature>;

    /// Perform the actual behavior of the plugin
    ///
    /// The behavior of the plugin is defined by the implementation of this method.
    /// When Nushell invoked the plugin [serve_plugin] will call this method and
    /// print the serialized returned value or error to stdout, which Nushell will
    /// interpret.
    ///
    /// The `name` is only relevant for plugins that implement multiple commands as the
    /// invoked command will be passed in via this argument. The `call` contains
    /// metadata describing how the plugin was invoked and `input` contains the structured
    /// data passed to the command implemented by this [Plugin].
    ///
    /// This variant expects to receive and produce [PipelineData], which allows for stream-based
    /// handling of I/O. This is recommended if the plugin is expected to transform large lists or
    /// potentially large quantities of bytes. The API is more complex however, and [Plugin] is
    /// recommended instead if this is not a concern.
    fn run(
        &mut self,
        name: &str,
        config: &Option<Value>,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError>;
}

/// All [Plugin]s can be used as [StreamingPlugin]s, but input streams will be fully consumed
/// before the plugin runs.
impl<T: Plugin> StreamingPlugin for T {
    fn signature(&self) -> Vec<PluginSignature> {
        <Self as Plugin>::signature(self)
    }

    fn run(
        &mut self,
        name: &str,
        config: &Option<Value>,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        // Unwrap the PipelineData from input, consuming the potential stream, and pass it to the
        // simpler signature in Plugin
        let span = input.span().unwrap_or(call.head);
        let input_value = input.into_value(span);
        // Wrap the output in PipelineData::Value
        <Self as Plugin>::run(self, name, config, call, &input_value)
            .map(|value| PipelineData::Value(value, None))
    }
}

/// Function used to implement the communication protocol between
/// nushell and an external plugin. Both [Plugin] and [StreamingPlugin] are supported.
///
/// When creating a new plugin this function is typically used as the main entry
/// point for the plugin, e.g.
///
/// ```
/// # use nu_plugin::*;
/// # use nu_protocol::{PluginSignature, Value};
/// # struct MyPlugin;
/// # impl MyPlugin { fn new() -> Self { Self }}
/// # impl Plugin for MyPlugin {
/// #     fn signature(&self) -> Vec<PluginSignature> {todo!();}
/// #     fn run(&mut self, name: &str, config: &Option<Value>, call: &EvaluatedCall, input: &Value)
/// #         -> Result<Value, LabeledError> {todo!();}
/// # }
/// fn main() {
///    serve_plugin(&mut MyPlugin::new(), MsgPackSerializer)
/// }
/// ```
pub fn serve_plugin(plugin: &mut impl StreamingPlugin, encoder: impl PluginEncoder + 'static) {
    if env::args().any(|arg| (arg == "-h") || (arg == "--help")) {
        print_help(plugin, encoder);
        std::process::exit(0)
    }

    // tell nushell encoding.
    //
    //                         1 byte
    // encoding format: |  content-length  | content    |
    let mut stdout = std::io::stdout();
    {
        let encoding = encoder.name();
        let length = encoding.len() as u8;
        let mut encoding_content: Vec<u8> = encoding.as_bytes().to_vec();
        encoding_content.insert(0, length);
        stdout
            .write_all(&encoding_content)
            .expect("Failed to tell nushell my encoding");
        stdout
            .flush()
            .expect("Failed to tell nushell my encoding when flushing stdout");
    }

    let mut manager = EngineInterfaceManager::new((stdout, encoder.clone()));
    let call_receiver = manager.take_plugin_call_receiver()
        // This expect should be totally safe, as we just created the manager
        .expect("take_plugin_call_receiver returned None");

    // We need to hold on to the interface to keep the manager alive. We can drop it at the end
    let interface = manager.get_interface();

    // Try an operation that could result in ShellError. Exit if an I/O error is encountered.
    // Try to report the error to nushell otherwise, and failing that, panic.
    macro_rules! try_or_report {
        ($interface:expr, $expr:expr) => (match $expr {
            Ok(val) => val,
            // Just exit if there is an I/O error. Most likely this just means that nushell
            // interrupted us. If not, the error probably happened on the other side too, so we
            // don't need to also report it.
            Err(ShellError::IOError { .. }) => std::process::exit(1),
            // If there is another error, try to send it to nushell and then exit.
            Err(err) => {
                $interface.write_response(Err(err.clone())).unwrap_or_else(|_| {
                    // If we can't send it to nushell, panic with it so at least we get the output
                    panic!("{}", err)
                });
                std::process::exit(1)
            }
        })
    }

    // Spawn the reader thread
    std::thread::spawn(move || {
        if let Err(err) = manager.consume_all((std::io::stdin().lock(), encoder)) {
            // Do our best to report the read error. Most likely there is some kind of
            // incompatibility between the plugin and nushell, so it makes more sense to try to
            // report it on stderr than to send something.
            let exe = std::env::current_exe().ok();

            let plugin_name: String = exe
                .as_ref()
                .and_then(|path| path.file_stem())
                .map(|stem| stem.to_string_lossy().into_owned())
                .map(|stem| stem.strip_prefix("nu_plugin_").map(|s| s.to_owned()).unwrap_or(stem))
                .unwrap_or_else(|| "(unknown)".into());

            eprintln!("Plugin `{plugin_name}` read error: {err}");
            std::process::exit(1);
        }
    });

    for plugin_call in call_receiver {
        match plugin_call {
            // Sending the signature back to nushell to create the declaration definition
            ReceivedPluginCall::Signature { engine } => {
                try_or_report!(engine, engine.write_signature(plugin.signature()));
            }
            // Run the plugin, handling any input or output streams
            ReceivedPluginCall::Run { engine, call: CallInfo { name, config, call, input } } => {
                let result = plugin.run(&name, &config, &engine, &call, input);
                try_or_report!(engine, engine.write_response(result));
            }
            // Collapse a custom value into plain nushell data
            ReceivedPluginCall::CollapseCustomValue { engine, plugin_data } => {
                let result = bincode::deserialize::<Box<dyn CustomValue>>(&plugin_data.data)
                    .map_err(|err| ShellError::PluginFailedToDecode {
                        msg: err.to_string()
                    })
                    // If deserialize succeeded, call to_base_value() and then make PipelineData
                    // to send the response.
                    .and_then(|value| value.to_base_value(plugin_data.span))
                    .map(|value| PipelineData::Value(value, None));
                try_or_report!(engine, engine.write_response(result));
            }
        }
    }

    // This will stop the manager
    drop(interface);
}

fn print_help(plugin: &mut impl StreamingPlugin, encoder: impl PluginEncoder) {
    println!("Nushell Plugin");
    println!("Encoder: {}", encoder.name());

    let mut help = String::new();

    plugin.signature().iter().for_each(|signature| {
        let res = write!(help, "\nCommand: {}", signature.sig.name)
            .and_then(|_| writeln!(help, "\nUsage:\n > {}", signature.sig.usage))
            .and_then(|_| {
                if !signature.sig.extra_usage.is_empty() {
                    writeln!(help, "\nExtra usage:\n > {}", signature.sig.extra_usage)
                } else {
                    Ok(())
                }
            })
            .and_then(|_| {
                let flags = get_flags_section(None, &signature.sig, |v| format!("{:#?}", v));
                write!(help, "{flags}")
            })
            .and_then(|_| writeln!(help, "\nParameters:"))
            .and_then(|_| {
                signature
                    .sig
                    .required_positional
                    .iter()
                    .try_for_each(|positional| {
                        writeln!(
                            help,
                            "  {} <{}>: {}",
                            positional.name, positional.shape, positional.desc
                        )
                    })
            })
            .and_then(|_| {
                signature
                    .sig
                    .optional_positional
                    .iter()
                    .try_for_each(|positional| {
                        writeln!(
                            help,
                            "  (optional) {} <{}>: {}",
                            positional.name, positional.shape, positional.desc
                        )
                    })
            })
            .and_then(|_| {
                if let Some(rest_positional) = &signature.sig.rest_positional {
                    writeln!(
                        help,
                        "  ...{} <{}>: {}",
                        rest_positional.name, rest_positional.shape, rest_positional.desc
                    )
                } else {
                    Ok(())
                }
            })
            .and_then(|_| writeln!(help, "======================"));

        if res.is_err() {
            println!("{res:?}")
        }
    });

    println!("{help}")
}

pub fn get_plugin_encoding(child_stdout: &mut ChildStdout) -> Result<EncodingType, ShellError> {
    let mut length_buf = [0u8; 1];
    child_stdout
        .read_exact(&mut length_buf)
        .map_err(|e| ShellError::PluginFailedToLoad {
            msg: format!("unable to get encoding from plugin: {e}"),
        })?;

    let mut buf = vec![0u8; length_buf[0] as usize];
    child_stdout
        .read_exact(&mut buf)
        .map_err(|e| ShellError::PluginFailedToLoad {
            msg: format!("unable to get encoding from plugin: {e}"),
        })?;

    EncodingType::try_from_bytes(&buf).ok_or_else(|| {
        let encoding_for_debug = String::from_utf8_lossy(&buf);
        ShellError::PluginFailedToLoad {
            msg: format!("get unsupported plugin encoding: {encoding_for_debug}"),
        }
    })
}
