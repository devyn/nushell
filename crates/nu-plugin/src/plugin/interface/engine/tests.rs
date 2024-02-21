use nu_protocol::{ShellError, Span, Value};

use crate::{
    plugin::interface::{test_util::TestCase, InterfaceManager},
    protocol::{
        ExternalStreamInfo, ListStreamInfo, PipelineDataHeader, PluginInput, Protocol,
        ProtocolInfo, RawStreamInfo, StreamMessage,
    },
};

#[test]
fn manager_consume_all_consumes_messages() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.engine();

    // This message should be non-problematic
    test.add(PluginInput::Hello(ProtocolInfo::default()));

    manager.consume_all(&mut test)?;

    assert!(!test.has_unconsumed_read());
    Ok(())
}

#[test]
fn manager_consume_all_exits_after_streams_and_interfaces_are_dropped() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.engine();

    // Add messages that won't cause errors
    for _ in 0..5 {
        test.add(PluginInput::Hello(ProtocolInfo::default()));
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
fn manager_consume_all_propagates_error_to_readers() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.engine();

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

fn invalid_input() -> PluginInput {
    // This should definitely cause an error, as 0.0.0 is not compatible with any version other than
    // itself
    PluginInput::Hello(ProtocolInfo {
        protocol: Protocol::NuPlugin,
        version: "0.0.0".into(),
        features: vec![],
    })
}

fn check_invalid_input_error(error: &ShellError) {
    // the error message should include something about the version...
    assert!(format!("{error:?}").contains("0.0.0"), "error: {error}");
}

#[test]
fn manager_consume_all_propagates_message_error_to_readers() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = test.engine();

    test.add(invalid_input());

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
        check_invalid_input_error(&error);
        Ok(())
    } else {
        panic!("did not get an error");
    }
}

#[test]
fn manager_consume_sets_protocol_info_on_hello() -> Result<(), ShellError> {
    let mut manager = TestCase::new().engine();

    let info = ProtocolInfo::default();

    manager.consume(PluginInput::Hello(info.clone()))?;

    let set_info = manager
        .protocol_info
        .as_ref()
        .expect("protocol info not set");
    assert_eq!(info.version, set_info.version);
    Ok(())
}

#[test]
fn manager_consume_errors_on_wrong_nushell_version() -> Result<(), ShellError> {
    let mut manager = TestCase::new().engine();

    let info = ProtocolInfo {
        protocol: Protocol::NuPlugin,
        version: "0.0.0".into(),
        features: vec![],
    };

    manager
        .consume(PluginInput::Hello(info))
        .expect_err("version 0.0.0 should cause an error");
    Ok(())
}

#[test]
fn manager_consume_errors_on_sending_other_messages_before_hello() -> Result<(), ShellError> {
    let mut manager = TestCase::new().engine();

    // hello not set
    assert!(manager.protocol_info.is_none());

    let error = manager
        .consume(PluginInput::Stream(StreamMessage::Drop(0)))
        .expect_err("consume before Hello should cause an error");

    assert!(format!("{error:?}").contains("Hello"));
    Ok(())
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_signature_forwards_to_receiver_with_context() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_run_forwards_to_receiver_with_context() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_run_forwards_to_receiver_with_pipeline_data() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_run_deserializes_custom_values_in_args() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_consume_call_custom_value_op_forwards_to_receiver_with_context() -> Result<(), ShellError>
{
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_prepare_pipeline_data_deserializes_custom_values() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_prepare_pipeline_data_deserializes_custom_values_in_streams() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn manager_prepare_pipeline_data_embeds_deserialization_errors_in_streams() -> Result<(), ShellError>
{
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_hello_sends_protocol_info() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_write_response_with_value() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_write_response_with_stream() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_write_response_with_error() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_write_signature() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_serializes_custom_values() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_serializes_custom_values_in_streams() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn interface_prepare_pipeline_data_embeds_serialization_errors_in_streams() -> Result<(), ShellError>
{
    todo!()
}
