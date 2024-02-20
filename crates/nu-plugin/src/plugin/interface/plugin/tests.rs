use std::sync::mpsc;

use nu_protocol::{ShellError, Span, Value};

use crate::{
    plugin::interface::{test_util::TestCase, InterfaceManager},
    protocol::{
        ExternalStreamInfo, ListStreamInfo, PipelineDataHeader, PluginCallId, Protocol,
        ProtocolInfo, RawStreamInfo,
    },
    PluginOutput,
};

use super::{PluginCallSubscription, PluginInterfaceManager, ReceivedPluginCallMessage};

#[test]
fn manager_consume_all_consumes_messages() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");

    // This message should be non-problematic
    test.add(PluginOutput::Hello(ProtocolInfo::default()));

    manager.consume_all(&mut test)?;

    assert!(!test.has_unconsumed_read());
    Ok(())
}

#[test]
fn manager_consume_all_exits_after_streams_and_interfaces_are_dropped() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");

    // Add messages that won't cause errors
    for _ in 0..5 {
        test.add(PluginOutput::Hello(ProtocolInfo::default()));
    }

    // Create a stream...
    let stream = manager.read_pipeline_data(
        PipelineDataHeader::ListStream(ListStreamInfo { id: 0 }),
        None,
    )?;

    // and an interface...
    let interface = manager.get_interface();

    // Expect that is_finished is false
    assert!(
        !manager.is_finished(),
        "is_finished is true even though active stream/interface exists"
    );

    // After dropping, it should be true
    drop(stream);
    drop(interface);

    assert!(
        manager.is_finished(),
        "is_finished is false even though manager has no stream or interface"
    );

    // When it's true, consume_all shouldn't consume everything
    manager.consume_all(&mut test)?;

    assert!(
        test.has_unconsumed_read(),
        "consume_all consumed the messages"
    );
    Ok(())
}

fn test_io_error() -> ShellError {
    ShellError::IOError {
        msg: "test io error".into(),
    }
}

fn check_test_io_error(error: &ShellError) {
    assert!(
        format!("{error:?}").contains("test io error"),
        "error: {error}"
    );
}

#[test]
fn manager_consume_all_propagates_io_error_to_readers() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");

    test.set_read_error(test_io_error());

    let stream = manager.read_pipeline_data(
        PipelineDataHeader::ListStream(ListStreamInfo { id: 0 }),
        None,
    )?;

    manager
        .consume_all(&mut test)
        .expect_err("consume_all did not error");

    // Ensure end of stream
    drop(manager);

    let value = stream.into_iter().next().expect("stream is empty");
    if let Value::Error { error, .. } = value {
        check_test_io_error(&error);
        Ok(())
    } else {
        panic!("did not get an error");
    }
}

fn invalid_output() -> PluginOutput {
    // This should definitely cause an error, as 0.0.0 is not compatible with any version other than
    // itself
    PluginOutput::Hello(ProtocolInfo {
        protocol: Protocol::NuPlugin,
        version: "0.0.0".into(),
        features: vec![],
    })
}

fn check_invalid_output_error(error: &ShellError) {
    // the error message should include something about the version...
    assert!(format!("{error:?}").contains("0.0.0"), "error: {error}");
}

#[test]
fn manager_consume_all_propagates_message_error_to_readers() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");

    test.add(invalid_output());

    let stream = manager.read_pipeline_data(
        PipelineDataHeader::ExternalStream(ExternalStreamInfo {
            span: Span::test_data(),
            stdout: Some(RawStreamInfo {
                id: 0,
                is_binary: false,
                known_size: None,
            }),
            stderr: None,
            exit_code: None,
            trim_end_newline: false,
        }),
        None,
    )?;

    manager
        .consume_all(&mut test)
        .expect_err("consume_all did not error");

    // Ensure end of stream
    drop(manager);

    let value = stream.into_iter().next().expect("stream is empty");
    if let Value::Error { error, .. } = value {
        check_invalid_output_error(&error);
        Ok(())
    } else {
        panic!("did not get an error");
    }
}

fn fake_plugin_call(
    manager: &mut PluginInterfaceManager,
    id: PluginCallId,
) -> mpsc::Receiver<ReceivedPluginCallMessage> {
    // Set up a fake plugin call subscription
    let (tx, rx) = mpsc::channel();

    manager.plugin_call_subscriptions.insert(
        id,
        PluginCallSubscription {
            sender: tx,
            context: None,
        },
    );

    rx
}

#[test]
fn manager_consume_all_propagates_io_error_to_plugin_calls() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");
    let interface = manager.get_interface();

    test.set_read_error(test_io_error());

    // Set up a fake plugin call subscription
    let rx = fake_plugin_call(&mut manager, 0);

    manager
        .consume_all(&mut test)
        .expect_err("consume_all did not error");

    // We have to hold interface until now otherwise consume_all won't try to process the message
    drop(interface);

    let message = rx.try_recv().expect("failed to get plugin call message");
    match message {
        ReceivedPluginCallMessage::Error(error) => {
            check_test_io_error(&error);
            Ok(())
        }
        _ => panic!("received something other than an error: {message:?}"),
    }
}

#[test]
fn manager_consume_all_propagates_message_error_to_plugin_calls() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.plugin("test");
    let interface = manager.get_interface();

    test.add(invalid_output());

    // Set up a fake plugin call subscription
    let rx = fake_plugin_call(&mut manager, 0);

    manager
        .consume_all(&mut test)
        .expect_err("consume_all did not error");

    // We have to hold interface until now otherwise consume_all won't try to process the message
    drop(interface);

    let message = rx.try_recv().expect("failed to get plugin call message");
    match message {
        ReceivedPluginCallMessage::Error(error) => {
            check_invalid_output_error(&error);
            Ok(())
        }
        _ => panic!("received something other than an error: {message:?}"),
    }
}

#[test]
#[ignore = "TODO"]
fn manager_consume_sets_protocol_info_on_hello() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_errors_on_wrong_nushell_version() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_errors_on_sending_other_messages_before_hello() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_response_forwards_to_subscriber_with_pipeline_data(
) -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_prepare_pipeline_data_adds_source_to_values() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_prepare_pipeline_data_adds_source_to_list_streams() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_hello_sends_protocol_info() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_writes_signature() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_writes_custom_value_op() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_writes_run_with_value_input() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_writes_run_with_stream_input() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_receives_response() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_plugin_call_receives_error() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_get_signature_round_trip() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_run_round_trip() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_custom_value_to_base_value_round_trip() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_accepts_normal_values() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_accepts_normal_streams() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_rejects_bad_custom_value() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_rejects_bad_custom_value_in_a_stream() -> Result<(), ShellError>
{
    todo!()
}
