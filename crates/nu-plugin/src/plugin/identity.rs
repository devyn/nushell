use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
};

use nu_protocol::ShellError;

use super::{create_command, make_plugin_interface, PluginInterface};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginIdentity {
    /// The filename used to start the plugin
    pub(crate) filename: PathBuf,
    /// The shell used to start the plugin, if required
    pub(crate) shell: Option<PathBuf>,
    /// The friendly name of the plugin (e.g. `inc` for `C:\nu_plugin_inc.exe`)
    pub(crate) plugin_name: String,
}

impl PluginIdentity {
    pub(crate) fn new(
        filename: impl Into<PathBuf>,
        shell: Option<impl Into<PathBuf>>,
    ) -> PluginIdentity {
        let filename = filename.into();
        let shell = shell.map(|s| s.into());
        // `C:\nu_plugin_inc.exe` becomes `inc`
        // `/home/nu/.cargo/bin/nu_plugin_inc` becomes `inc`
        // `/home/nu/other_inc` becomes `other_inc` as a fallback
        // a path not having a file stem, like an empty path, becomes `<unknown>`
        let plugin_name = filename
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .map(|stem| {
                stem.strip_prefix("nu_plugin_")
                    .map(|s| s.to_owned())
                    .unwrap_or(stem)
            })
            .unwrap_or_else(|| "<unknown>".into());
        PluginIdentity {
            filename,
            shell,
            plugin_name,
        }
    }

    pub(crate) fn spawn(
        self: Arc<Self>,
        envs: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
    ) -> Result<PluginInterface, ShellError> {
        let source_file = Path::new(&self.filename);
        let mut plugin_cmd = create_command(source_file, self.shell.as_deref());

        // We need the current environment variables for `python` based plugins
        // Or we'll likely have a problem when a plugin is implemented in a virtual Python environment.
        plugin_cmd.envs(envs);

        let program_name = plugin_cmd.get_program().to_os_string().into_string();

        // Run the plugin command
        let child = plugin_cmd.spawn().map_err(|err| {
            let error_msg = match err.kind() {
                std::io::ErrorKind::NotFound => match program_name {
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

        make_plugin_interface(child, self)
    }
}
