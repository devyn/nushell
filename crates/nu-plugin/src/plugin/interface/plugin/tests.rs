use std::path::Path;
use std::sync::Arc;

use crate::plugin::context::PluginExecutionContext;
use crate::plugin::interface::{
    buffers::TypedStreamBuffer,
    stream_data_io::{gen_stream_data_tests, StreamDataIo, StreamDataIoTestExt},
    test_util::TestCase,
    PluginInterface,
};
use crate::protocol::{
    CallInfo, ExternalStreamInfo, ListStreamInfo, PipelineDataHeader, PluginCall,
    PluginCallResponse, PluginCustomValue, PluginData, PluginInput, PluginOutput, RawStreamInfo,
    StreamData, StreamId, StreamMessage,
};

use super::PluginInterfaceIo;

use nu_protocol::engine::Closure;
use nu_protocol::{ListStream, PipelineData, ShellError, Span, Spanned, Value};

gen_stream_data_tests!(
    PluginOutput(add_output),
    PluginInput(next_written_input),
    |test| test.plugin_interface_impl(None)
);

struct BogusContext;
impl PluginExecutionContext for BogusContext {
    fn filename(&self) -> &Path {
        Path::new("/bogus")
    }

    fn shell(&self) -> Option<&Path> {
        None
    }

    fn command_span(&self) -> nu_protocol::Span {
        Span::test_data()
    }

    fn command_name(&self) -> &str {
        "bogus"
    }

    fn ctrlc(&self) -> Option<&std::sync::Arc<std::sync::atomic::AtomicBool>> {
        None
    }

    fn get_config(&self) -> &nu_protocol::Config {
        panic!("get_config not implemented")
    }

    fn eval_closure(
        &self,
        closure: Spanned<Closure>,
        positional: Vec<Value>,
        input: PipelineData,
        redirect_stdout: bool,
        redirect_stderr: bool,
    ) -> Result<PipelineData, ShellError> {
        let span = input.span().unwrap_or(Span::test_data());

        Ok(PipelineData::Value(
            Value::test_list(vec![
                Value::closure(closure.item, closure.span),
                Value::test_list(positional),
                input.into_value(span),
                Value::test_bool(redirect_stdout),
                Value::test_bool(redirect_stderr),
            ]),
            None,
        ))
    }
}

#[test]
fn get_context() {
    let test = TestCase::new();
    let interface = test.plugin_interface_impl(None);
    assert!(interface.context().is_none());
    let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
    assert_eq!(
        "bogus",
        interface
            .context()
            .expect("context should be set")
            .command_name()
    );
}

#[test]
fn write_call() {
    let test = TestCase::new();
    let interface = test.plugin_interface_impl(None);
    interface
        .write_call(PluginCall::Signature)
        .expect("write_call failed");

    match test.next_written_input() {
        Some(PluginInput::Call(PluginCall::Signature)) => (),
        Some(other) => panic!("wrote wrong input: {other:?}"),
        None => panic!("didn't write anything"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn read_call_response_signature() {
    let test = TestCase::new();
    test.add_output(PluginOutput::CallResponse(PluginCallResponse::Signature(
        vec![],
    )));

    match test.plugin_interface(None).read_call_response().unwrap() {
        PluginCallResponse::Signature(vec) => assert!(vec.is_empty()),
        other => panic!("read unexpected response: {:?}", other),
    }
}

#[test]
fn read_call_response_empty() {
    let test = TestCase::new();
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::Empty),
    ));

    match test.plugin_interface(None).read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::Empty) => (),
        other => panic!("read unexpected response: {:?}", other),
    }
}

#[test]
fn read_call_response_value() {
    let test = TestCase::new();
    let value = Value::test_int(5);
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::Value(value.clone())),
    ));

    match test.plugin_interface(None).read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::Value(read_value)) => {
            assert_eq!(value, read_value)
        }
        other => panic!("read unexpected response: {:?}", other),
    }
}

#[test]
fn read_call_response_data() {
    let test = TestCase::new();
    let data = PluginData {
        name: Some("Foo".into()),
        data: vec![4, 6],
        span: Span::new(40, 60),
    };
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::PluginData(data.clone())),
    ));

    match test.plugin_interface(None).read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::PluginData(read_data)) => {
            assert_eq!(data, read_data);
        }
        other => panic!("read unexpected response: {:?}", other),
    }
}

#[test]
fn read_call_response_list_stream() {
    let test = TestCase::new();
    let info = ListStreamInfo { id: 7 };
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::ListStream(info.clone())),
    ));

    let interface = test.plugin_interface_impl(None);

    let read_info = match interface.clone().read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::ListStream(info)) => info,
        other => panic!("read unexpected response: {:?}", other),
    };
    assert_eq!(info, read_info);

    {
        let mut read = interface.read.lock().unwrap();
        let buf = read.stream_buffers.get(info.id).unwrap();
        assert!(matches!(buf, TypedStreamBuffer::List(..)));
    }
}

#[test]
fn read_call_response_external_stream() {
    let test = TestCase::new();
    let info = ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 0,
            is_binary: false,
            known_size: None,
        }),
        stderr: Some(RawStreamInfo {
            id: 1,
            is_binary: false,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 2 }),
        trim_end_newline: false,
    };
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::ExternalStream(info)),
    ));

    let interface = test.plugin_interface_impl(None);

    match interface.clone().read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::ExternalStream(info)) => {
            assert_eq!(0, info.stdout.as_ref().unwrap().id);
            assert_eq!(1, info.stderr.as_ref().unwrap().id);
            assert_eq!(2, info.exit_code.as_ref().unwrap().id);
        }
        other => panic!("read unexpected response: {:?}", other),
    }

    {
        let mut read = interface.read.lock().unwrap();

        let buf = read.stream_buffers.get(0).expect("stdout missing");
        if let TypedStreamBuffer::Raw(buf) = buf {
            assert!(!buf.is_dropped());
        } else {
            panic!("stdout not raw");
        }

        let buf = read.stream_buffers.get(1).expect("stderr missing");
        if let TypedStreamBuffer::Raw(buf) = buf {
            assert!(!buf.is_dropped());
        } else {
            panic!("stderr not raw");
        }

        let buf = read.stream_buffers.get(2).expect("exit_code missing");
        if let TypedStreamBuffer::List(buf) = buf {
            assert!(!buf.is_dropped());
        } else {
            panic!("exit_code not list");
        }
    }
}

#[test]
fn read_call_response_unexpected_stream_data() {
    let test = TestCase::new();
    test.add_output(StreamMessage::Data(0, StreamData::List(None)));
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::Empty),
    ));

    test.plugin_interface(None)
        .read_call_response()
        .expect_err("should be an error");
}

#[derive(Clone, Copy)]
enum ExpectType {
    None,
    List,
    Raw,
}

fn validate_stream_data_acceptance(header: PipelineDataHeader, ids: &[(StreamId, ExpectType)]) {
    let test = TestCase::new();
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(header),
    ));

    let interface = test.plugin_interface_impl(None);

    interface.clone().read_call_response().expect("call failed");

    for (id, expect_type) in ids.iter().cloned() {
        test.clear_output();
        test.add_output(StreamMessage::Data(
            id,
            StreamData::List(Some(Value::test_bool(true))),
        ));
        let result = interface.clone().read_list(id);

        if matches!(expect_type, ExpectType::List) {
            assert!(
                result.is_ok(),
                "stream {id} should accept list: {}",
                result.unwrap_err()
            );
        } else {
            assert!(result.is_err(), "stream {id} should not accept list");
        }

        test.clear_output();
        test.add_output(StreamMessage::Data(
            id,
            StreamData::Raw(Some(Ok(vec![]))),
        ));
        let result = interface.clone().read_raw(id);

        if matches!(expect_type, ExpectType::Raw) {
            assert!(
                result.is_ok(),
                "stream {id} should accept raw: {}",
                result.unwrap_err()
            );
        } else {
            assert!(result.is_err(), "stream {id} should not accept raw");
        }
    }
}

#[test]
fn read_call_response_with_empty_accepts_nothing() {
    validate_stream_data_acceptance(PipelineDataHeader::Empty, &[(0, ExpectType::None)])
}

#[test]
fn read_call_response_with_list_stream_input_accepts_only_list_stream_data() {
    validate_stream_data_acceptance(
        PipelineDataHeader::ListStream(ListStreamInfo { id: 2 }),
        &[(2, ExpectType::List)],
    )
}

#[test]
fn read_call_response_with_external_stream_stdout_input_accepts_only_raw_data() {
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 3,
            is_binary: false,
            known_size: None,
        }),
        stderr: None,
        exit_code: None,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(
        header,
        &[
            (3, ExpectType::Raw),
            (4, ExpectType::None),
            (5, ExpectType::None),
        ],
    )
}

#[test]
fn read_call_response_with_external_stream_stderr_input_accepts_only_raw_data() {
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: None,
        stderr: Some(RawStreamInfo {
            id: 4,
            is_binary: false,
            known_size: None,
        }),
        exit_code: None,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(
        header,
        &[
            (3, ExpectType::None),
            (4, ExpectType::Raw),
            (5, ExpectType::None),
        ],
    )
}

#[test]
fn read_call_response_with_external_stream_exit_code_input_accepts_only_list_data() {
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: None,
        stderr: None,
        exit_code: Some(ListStreamInfo { id: 5 }),
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(
        header,
        &[
            (3, ExpectType::None),
            (4, ExpectType::None),
            (5, ExpectType::List),
        ],
    )
}

#[test]
fn read_call_response_with_external_stream_all_input_accepts_only_all_external_stream_data() {
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 3,
            is_binary: false,
            known_size: None,
        }),
        stderr: Some(RawStreamInfo {
            id: 4,
            is_binary: false,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 5 }),
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(
        header,
        &[
            (3, ExpectType::Raw),
            (4, ExpectType::Raw),
            (5, ExpectType::List),
        ],
    )
}

#[test]
fn read_call_response_with_overlapping_external_stream_ids_causes_error() {
    let test = TestCase::new();
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 7,
            is_binary: false,
            known_size: None,
        }),
        stderr: Some(RawStreamInfo {
            id: 7,
            is_binary: false,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 8 }),
        trim_end_newline: false,
    });
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(header),
    ));

    let interface = test.plugin_interface_impl(None);

    interface
        .clone()
        .read_call_response()
        .expect_err("call succeeded");
}

#[test]
fn read_call_response_end_of_input() {
    let test = TestCase::new();
    test.plugin_interface(None)
        .read_call_response()
        .expect_err("should fail");
}

#[test]
fn read_call_response_io_error() {
    let test = TestCase::new();
    test.set_read_error(ShellError::IOError {
        msg: "test error".into(),
    });

    match test
        .plugin_interface(None)
        .read_call_response()
        .expect_err("should be an error")
    {
        ShellError::IOError { msg } if msg == "test error" => (),
        other => panic!("got some other error: {other}"),
    }
}

#[test]
fn make_pipeline_data_empty() {
    let test = TestCase::new();

    let pipe = test
        .plugin_interface(Some(Arc::new(BogusContext)))
        .make_pipeline_data(PipelineDataHeader::Empty)
        .expect("can't make pipeline data");

    match pipe {
        PipelineData::Empty => (),
        PipelineData::Value(_, _) => panic!("got value, expected empty"),
        PipelineData::ListStream(_, _) => panic!("got list stream"),
        PipelineData::ExternalStream { .. } => panic!("got external stream"),
    }
}

#[test]
fn make_pipeline_data_value() {
    let test = TestCase::new();

    let value = Value::test_int(2);
    let header = PipelineDataHeader::Value(value.clone().into());
    let pipe = test
        .plugin_interface(Some(Arc::new(BogusContext)))
        .make_pipeline_data(header)
        .expect("can't make pipeline data");

    match pipe {
        PipelineData::Empty => panic!("got empty, expected value"),
        PipelineData::Value(v, _) => assert_eq!(value, v),
        PipelineData::ListStream(_, _) => panic!("got list stream"),
        PipelineData::ExternalStream { .. } => panic!("got external stream"),
    }
}

#[test]
fn make_pipeline_data_custom_data() {
    let test = TestCase::new();

    let data = PluginData {
        name: Some("Foo".into()),
        data: vec![32, 40, 80],
        span: Span::test_data(),
    };
    let header = PipelineDataHeader::PluginData(data.clone());

    let pipe = test
        .plugin_interface(Some(Arc::new(BogusContext)))
        .make_pipeline_data(header)
        .expect("failed to make pipeline data");

    match pipe {
        PipelineData::Empty => panic!("got empty, expected value"),
        PipelineData::Value(v, _) => {
            assert_eq!(data.span, v.span());

            let read_custom = v.as_custom_value().expect("not a custom value");
            let read_downcast: &PluginCustomValue =
                read_custom.as_any().downcast_ref().expect("wrong type");

            assert_eq!(data.name.unwrap(), read_downcast.name);
            assert_eq!("/bogus", read_downcast.filename.display().to_string());
            assert_eq!(None, read_downcast.shell);
            assert_eq!(data.data, read_downcast.data);
            assert_eq!("bogus", read_downcast.source);
        }
        PipelineData::ListStream(_, _) => panic!("got list stream"),
        PipelineData::ExternalStream { .. } => panic!("got external stream"),
    }
}

#[test]
fn make_pipeline_data_list_stream() {
    let test = TestCase::new();

    let values = [Value::test_int(4), Value::test_string("hello")];

    for value in &values {
        test.add_output(StreamMessage::Data(
            0,
            StreamData::List(Some(value.clone())),
        ));
    }
    // end
    test.add_output(StreamMessage::Data(0, StreamData::List(None)));

    let header = PipelineDataHeader::ListStream(ListStreamInfo { id: 0 });

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        interface.def_list(0);
        interface
    });

    let pipe = interface
        .make_pipeline_data(header)
        .expect("failed to make pipeline data");

    assert!(matches!(pipe, PipelineData::ListStream(..)));

    for (defined_value, read_value) in values.into_iter().zip(pipe.into_iter()) {
        assert_eq!(defined_value, read_value);
    }
}

#[test]
fn make_pipeline_data_external_stream() {
    let test = TestCase::new();

    // Test many simultaneous streams out of order
    let stream_data = [
        (0, StreamData::Raw(Some(Ok(b"foo".to_vec())))),
        (1, StreamData::Raw(Some(Ok(b"bar".to_vec())))),
        (2, StreamData::List(Some(Value::test_int(1)))),
        (1, StreamData::Raw(Some(Ok(b"barr".to_vec())))),
        (1, StreamData::Raw(None)),
        (0, StreamData::Raw(Some(Ok(b"fooo".to_vec())))),
        (0, StreamData::Raw(None)),
        (2, StreamData::List(None)),
    ];

    for (id, data) in stream_data {
        test.add_output(StreamMessage::Data(id, data));
    }

    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 0,
            is_binary: true,
            known_size: Some(7),
        }),
        stderr: Some(RawStreamInfo {
            id: 1,
            is_binary: false,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 2 }),
        trim_end_newline: false,
    });

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        interface.def_external(0, 1, 2);
        interface
    });

    let pipe = interface
        .make_pipeline_data(header)
        .expect("failed to make pipeline data");

    match pipe {
        PipelineData::ExternalStream {
            stdout,
            stderr,
            exit_code,
            span,
            trim_end_newline,
            ..
        } => {
            assert!(stdout.is_some());
            assert!(stderr.is_some());
            assert!(exit_code.is_some());
            assert_eq!(Span::test_data(), span, "span");
            assert!(!trim_end_newline);

            if let Some(rs) = stdout.as_ref() {
                assert!(rs.is_binary, "stdout.is_binary=false");
                assert_eq!(Some(7), rs.known_size, "stdout.known_size");
            }
            if let Some(rs) = stderr.as_ref() {
                assert!(!rs.is_binary, "stderr.is_binary=true");
                assert_eq!(None, rs.known_size, "stderr.known_size");
            }

            let out_bytes = stdout.unwrap().into_bytes().expect("failed to read stdout");
            let err_bytes = stderr.unwrap().into_bytes().expect("failed to read stderr");
            let exit_code_vals: Vec<_> = exit_code.unwrap().collect();

            assert_eq!(b"foofooo", &out_bytes.item[..]);
            assert_eq!(b"barbarr", &err_bytes.item[..]);
            assert_eq!(vec![Value::test_int(1)], exit_code_vals);
        }
        PipelineData::Empty => panic!("expected external stream, got empty"),
        PipelineData::Value(..) => panic!("expected external stream, got value"),
        PipelineData::ListStream(..) => panic!("expected external stream, got list stream"),
    }
}

#[test]
fn make_pipeline_data_external_stream_error() {
    let test = TestCase::new();

    // Just test stdout, but with an error
    let spec_msg = "failure";
    let stream_data = [
        (44, StreamData::List(Some(Value::int(1, Span::test_data())))),
        (
            42,
            StreamData::Raw(Some(Err(ShellError::NushellFailed {
                msg: spec_msg.into(),
            }))),
        ),
        (42, StreamData::Raw(None)),
    ];

    for (id, data) in stream_data {
        test.add_output(StreamMessage::Data(id, data));
    }

    // Still enable the other streams, to ensure ignoring the other data works
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            id: 42,
            is_binary: false,
            known_size: None,
        }),
        stderr: Some(RawStreamInfo {
            id: 43,
            is_binary: false,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 44 }),
        trim_end_newline: false,
    });

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        interface.def_external(42, 43, 44);
        interface
    });

    let pipe = interface
        .make_pipeline_data(header)
        .expect("failed to make pipeline data");

    match pipe {
        PipelineData::ExternalStream {
            stdout,
            stderr,
            exit_code,
            ..
        } => {
            assert!(stdout.is_some());
            assert!(stderr.is_some());
            assert!(exit_code.is_some());

            match stdout
                .unwrap()
                .into_bytes()
                .expect_err("stdout read successfully")
            {
                ShellError::NushellFailed { msg } => assert_eq!(spec_msg, msg),
                other => panic!("unexpected other error while reading stdout: {other}"),
            }
        }
        PipelineData::Empty => panic!("expected external stream, got empty"),
        PipelineData::Value(..) => panic!("expected external stream, got value"),
        PipelineData::ListStream(..) => panic!("expected external stream, got list stream"),
    }
}

#[test]
fn round_trip_list_stream() {
    // First, handle the write to plugin side
    let test_plugin = TestCase::new();
    let plug_interface = test_plugin.plugin_interface(Some(Arc::new(BogusContext)));

    let header = PipelineDataHeader::ListStream(ListStreamInfo { id: 0 });

    let call_info = CallInfo {
        name: "roundtrip".into(),
        call: crate::EvaluatedCall {
            head: Span::test_data(),
            positional: vec![],
            named: vec![],
        },
        input: header.clone(),
        config: None,
    };
    let call = PluginCall::Run(call_info.clone());

    let values = vec![Value::test_int(4), Value::test_string("hi")];

    plug_interface.write_call(call).unwrap();
    plug_interface
        .write_pipeline_data_stream(
            &header,
            PipelineData::ListStream(
                ListStream::from_stream(values.clone().into_iter(), None),
                None,
            ),
        )
        .unwrap();

    // Copy as input to engine side
    let test_engine = TestCase::new();
    let engine_interface = test_engine.engine_interface();
    test_engine.extend_input(test_plugin.written_inputs());

    let read_call = engine_interface
        .read_call()
        .expect("failed to read call")
        .expect("nothing to read");

    let read_input = match read_call {
        PluginCall::Run(read_call_info) => {
            assert_eq!(call_info.name, read_call_info.name);
            assert_eq!(call_info.call.head, read_call_info.call.head);
            assert_eq!(call_info.call.positional, read_call_info.call.positional);
            assert_eq!(call_info.call.named, read_call_info.call.named);
            assert_eq!(call_info.input, read_call_info.input);
            assert_eq!(call_info.config, read_call_info.config);
            read_call_info.input
        }
        other => panic!("unexpected call: {other:?}"),
    };

    // Read the values
    let pipe = engine_interface
        .make_pipeline_data(read_input)
        .expect("failed to make plugin input pipeline data");
    assert_eq!(values, pipe.into_iter().collect::<Vec<_>>());

    // Write response list stream
    let output_values = vec![Value::test_float(4.0), Value::test_string("hello there")];
    engine_interface
        .write_pipeline_data_response(PipelineData::ListStream(
            ListStream::from_stream(output_values.clone().into_iter(), None),
            None,
        ))
        .expect("failed to write pipeline data response");

    // Copy response back to plugin
    test_plugin.extend_output(test_engine.written_outputs());

    // Check the response header
    let response = plug_interface
        .read_call_response()
        .expect("failed to read response");

    let header = match response {
        PluginCallResponse::PipelineData(header) => header,
        other => panic!("incorrect plugin call response: {other:?}"),
    };

    // Read the values and check
    let pipe = plug_interface
        .make_pipeline_data(header)
        .expect("failed to make plugin output pipeline data");
    assert_eq!(output_values, pipe.into_iter().collect::<Vec<_>>());

    // Ensure all data consumed
    assert!(!test_plugin.has_unconsumed_read());
    assert!(!test_plugin.has_unconsumed_write());
    assert!(!test_engine.has_unconsumed_read());
    assert!(!test_engine.has_unconsumed_write());
}
