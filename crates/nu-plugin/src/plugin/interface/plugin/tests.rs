use crate::plugin::interface::stream_data_io::{
    StreamDataIo, StreamBuffers, gen_stream_data_tests
};
use crate::protocol::{StreamData, PluginInput, PluginOutput};
use crate::plugin::interface::test_util::TestCase;

use nu_protocol::{Value, ShellError};

gen_stream_data_tests!(
    PluginOutput (add_output),
    PluginInput (next_written_input),
    |test| test.plugin_interface_impl(None)
);
