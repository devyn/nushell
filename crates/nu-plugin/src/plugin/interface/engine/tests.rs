use nu_protocol::{CustomValue, ListStream, PipelineData, RawStream, ShellError, Span, Value};
use serde::{Deserialize, Serialize};

use crate::plugin::interface::buffers::TypedStreamBuffer;
use crate::plugin::interface::engine::EngineInterfaceIo;
use crate::plugin::interface::stream_data_io::{
    gen_stream_data_tests, StreamDataIo, StreamDataIoTestExt,
};
use crate::plugin::interface::test_util::TestCase;
use crate::protocol::{
    CallInfo, EvaluatedCall, ExternalStreamInfo, ListStreamInfo, PipelineDataHeader, PluginCall,
    PluginData, PluginInput, PluginOutput, RawStreamInfo, StreamId, StreamMessage, StreamData,
    PluginCallResponse,
};

use super::EngineInterface;

gen_stream_data_tests!(
    PluginInput(add_input),
    PluginOutput(next_written_output),
    |test| test.engine_interface_impl()
);

#[test]
fn read_call_signature() {
    let test = TestCase::new();
    test.add_input(PluginInput::Call(PluginCall::Signature));

    match test.engine_interface().read_call().unwrap() {
        Some(PluginCall::Signature) => (),
        Some(other) => panic!("read unexpected call: {:?}", other),
        None => panic!("end of input"),
    }
}

#[test]
fn read_call_run() {
    let test = TestCase::new();
    let call_info = CallInfo {
        name: "test call".into(),
        call: EvaluatedCall {
            head: Span::test_data(),
            positional: vec![],
            named: vec![],
        },
        input: PipelineDataHeader::Empty,
        config: None,
    };
    test.add_input(PluginInput::Call(PluginCall::Run(call_info.clone())));

    match test.engine_interface().read_call().unwrap() {
        Some(PluginCall::Run(read_call_info)) => {
            assert_eq!(call_info.name, read_call_info.name);
            assert_eq!(call_info.call.head, read_call_info.call.head);
            assert_eq!(call_info.call.positional, read_call_info.call.positional);
            assert_eq!(call_info.call.named, read_call_info.call.named);
            assert_eq!(call_info.input, read_call_info.input);
            assert_eq!(call_info.config, read_call_info.config);
        }
        Some(other) => panic!("read unexpected call: {:?}", other),
        None => panic!("end of input"),
    }
}

#[test]
fn read_call_collapse_custom_value() {
    let test = TestCase::new();
    let data = PluginData {
        name: None,
        data: vec![42, 13, 37],
        span: Span::test_data(),
    };
    test.add_input(PluginInput::Call(PluginCall::CollapseCustomValue(
        data.clone(),
    )));

    match test.engine_interface().read_call().unwrap() {
        Some(PluginCall::CollapseCustomValue(read_data)) => assert_eq!(data, read_data),
        Some(other) => panic!("read unexpected call: {:?}", other),
        None => panic!("end of input"),
    }
}

#[test]
fn read_call_unexpected_stream_data() {
    let test = TestCase::new();
    test.add_input(StreamMessage::Data(0, StreamData::List(None)));
    test.add_input(PluginInput::Call(PluginCall::Signature));

    test.engine_interface()
        .read_call()
        .expect_err("should be an error");
}

#[test]
fn read_call_ignore_dropped_stream_data() {
    let test = TestCase::new();
    test.add_input(StreamMessage::Data(0, StreamData::List(None)));
    test.add_input(PluginInput::Call(PluginCall::Signature));

    let interface = test.engine_interface_impl();
    interface.def_list(0);

    interface
        .clone()
        .read
        .lock()
        .unwrap()
        .stream_buffers
        .get(0)
        .unwrap()
        .drop_list();
    interface.clone().read_call().expect("should succeed");
}

fn test_call_with_input(input: PipelineDataHeader) -> CallInfo {
    CallInfo {
        name: "test call".into(),
        call: EvaluatedCall {
            head: Span::test_data(),
            positional: vec![],
            named: vec![],
        },
        input,
        config: None,
    }
}

#[derive(Clone, Copy)]
enum ExpectType {
    None,
    List,
    Raw,
}

fn validate_stream_data_acceptance(header: PipelineDataHeader, ids: &[(StreamId, ExpectType)]) {
    let test = TestCase::new();
    let call_info = test_call_with_input(header);
    test.add_input(PluginInput::Call(PluginCall::Run(call_info)));

    let interface = test.engine_interface_impl();

    interface.clone().read_call().expect("call failed");

    for (id, expect_type) in ids.iter().cloned() {
        test.clear_input();
        test.add_input(StreamMessage::Data(
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

        test.clear_input();
        test.add_input(StreamMessage::Data(
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
fn read_call_run_with_empty_accepts_nothing() {
    validate_stream_data_acceptance(PipelineDataHeader::Empty, &[(0, ExpectType::None)])
}

#[test]
fn read_call_run_with_list_stream_input_accepts_only_list_stream_data() {
    validate_stream_data_acceptance(
        PipelineDataHeader::ListStream(ListStreamInfo { id: 2 }),
        &[(2, ExpectType::List)],
    )
}

#[test]
fn read_call_run_with_external_stream_stdout_input_accepts_only_raw_data() {
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
fn read_call_run_with_external_stream_stderr_input_accepts_only_raw_data() {
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
fn read_call_run_with_external_stream_exit_code_input_accepts_only_list_data() {
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
fn read_call_run_with_external_stream_all_input_accepts_only_all_external_stream_data() {
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
fn read_call_run_with_overlapping_external_stream_ids_causes_error() {
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
    let call_info = test_call_with_input(header);
    test.add_input(PluginInput::Call(PluginCall::Run(call_info)));

    let interface = test.engine_interface_impl();

    interface.clone().read_call().expect_err("call succeeded");
}

#[test]
fn read_call_end_of_input() {
    let test = TestCase::new();
    if let Some(call) = test.engine_interface().read_call().expect("should succeed") {
        panic!("should have been end of input, but read {call:?}");
    }
}

#[test]
fn read_call_io_error() {
    let test = TestCase::new();
    test.set_read_error(ShellError::IOError {
        msg: "test error".into(),
    });

    match test
        .engine_interface()
        .read_call()
        .expect_err("should be an error")
    {
        ShellError::IOError { msg } if msg == "test error" => (),
        other => panic!("got some other error: {other}"),
    }
}

#[test]
fn write_call_response() {
    let test = TestCase::new();
    let response = PluginCallResponse::PipelineData(PipelineDataHeader::Empty);
    test.engine_interface()
        .write_call_response(response.clone())
        .expect("should succeed");
    match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::Empty,
        ))) => (),
        Some(other) => panic!("wrote the wrong message: {other:?}"),
        None => panic!("didn't write anything"),
    }
    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_call_response_error() {
    let test = TestCase::new();
    test.set_write_error(ShellError::IOError {
        msg: "test error".into(),
    });

    let response = PluginCallResponse::PipelineData(PipelineDataHeader::Empty);
    match test
        .engine_interface()
        .write_call_response(response)
        .expect_err("should be an error")
    {
        ShellError::IOError { msg } if msg == "test error" => (),
        other => panic!("got some other error: {other}"),
    }
    assert!(!test.has_unconsumed_write());
}

#[test]
fn make_pipeline_data_empty() {
    let test = TestCase::new();

    let pipe = test
        .engine_interface()
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
    let pipe = test
        .engine_interface()
        .make_pipeline_data(PipelineDataHeader::Value(value.clone()))
        .expect("can't make pipeline data");

    match pipe {
        PipelineData::Empty => panic!("got empty, expected value"),
        PipelineData::Value(v, _) => assert_eq!(value, v),
        PipelineData::ListStream(_, _) => panic!("got list stream"),
        PipelineData::ExternalStream { .. } => panic!("got external stream"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MyCustom(i32);

#[typetag::serde]
impl CustomValue for MyCustom {
    fn clone_value(&self, span: Span) -> Value {
        Value::custom_value(Box::new(self.clone()), span)
    }

    fn value_string(&self) -> String {
        self.0.to_string()
    }

    fn to_base_value(&self, span: Span) -> Result<Value, ShellError> {
        Ok(Value::int(self.0 as i64, span))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn make_pipeline_data_custom_data() {
    let test = TestCase::new();

    let custom: Box<dyn CustomValue> = Box::new(MyCustom(42));
    let bincoded = bincode::serialize(&custom).expect("serialization failed");

    let data = PluginData {
        name: None,
        data: bincoded,
        span: Span::test_data(),
    };
    let header = PipelineDataHeader::PluginData(data);

    let pipe = test
        .engine_interface()
        .make_pipeline_data(header)
        .expect("failed to make pipeline data");

    match pipe {
        PipelineData::Empty => panic!("got empty, expected value"),
        PipelineData::Value(v, _) => {
            let read_custom = v.as_custom_value().expect("not a custom value");
            let read_downcast: &MyCustom = read_custom.as_any().downcast_ref().expect("wrong type");
            assert_eq!(&MyCustom(42), read_downcast);
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
        test.add_input(StreamMessage::Data(
            0,
            StreamData::List(Some(value.clone())),
        ));
    }
    // end
    test.add_input(StreamMessage::Data(0, StreamData::List(None)));

    let header = PipelineDataHeader::ListStream(ListStreamInfo { id: 0 });

    let interface = EngineInterface::from({
        let interface = test.engine_interface_impl();
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
        test.add_input(StreamMessage::Data(id, data));
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

    let interface = EngineInterface::from({
        let interface = test.engine_interface_impl();
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
                assert!(!rs.is_binary, "stderr.is_binary=false");
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
        (2, StreamData::List(Some(Value::int(1, Span::test_data())))),
        (
            0,
            StreamData::Raw(Some(Err(ShellError::NushellFailed {
                msg: spec_msg.into(),
            }))),
        ),
        (0, StreamData::Raw(None)),
    ];

    for (id, data) in stream_data {
        test.add_input(StreamMessage::Data(id, data));
    }

    // Still enable the other streams, to ensure ignoring the other data works
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
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
    });

    let interface = EngineInterface::from({
        let interface = test.engine_interface_impl();
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
fn write_pipeline_data_response_empty() {
    let test = TestCase::new();
    test.engine_interface()
        .write_pipeline_data_response(PipelineData::Empty)
        .expect("failed to write empty response");

    match test.next_written_output() {
        Some(output) => match output {
            PluginOutput::CallResponse(PluginCallResponse::PipelineData(
                PipelineDataHeader::Empty,
            )) => (),
            PluginOutput::CallResponse(other) => panic!("unexpected response: {other:?}"),
            other => panic!("unexpected output: {other:?}"),
        },
        None => panic!("no response written"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_value() {
    let test = TestCase::new();
    let value = Value::test_string("hello");
    let data = PipelineData::Value(value.clone(), None);
    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write value response");

    match test.next_written_output() {
        Some(output) => match output {
            PluginOutput::CallResponse(PluginCallResponse::PipelineData(
                PipelineDataHeader::Value(v),
            )) => assert_eq!(value, v),
            PluginOutput::CallResponse(other) => panic!("unexpected response: {other:?}"),
            other => panic!("unexpected output: {other:?}"),
        },
        None => panic!("no response written"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_list_stream() {
    let test = TestCase::new();

    let values = vec![
        Value::test_int(4),
        Value::test_bool(false),
        Value::test_string("foobar"),
    ];

    let list_stream = ListStream::from_stream(values.clone().into_iter(), None);
    let data = PipelineData::ListStream(list_stream, None);
    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write list stream response");

    // Response starts by signaling a ListStream return value:
    let id = match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::ListStream(ListStreamInfo { id }),
        ))) => id,
        Some(other) => panic!("unexpected response: {other:?}"),
        None => panic!("response not written"),
    };

    // Followed by each stream value...
    for (expected_value, output) in values.into_iter().zip(test.written_outputs()) {
        match output {
            PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(Some(read_value))))
                if id == stream_id =>
            {
                assert_eq!(expected_value, read_value)
            }
            PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(None))) if id == stream_id => {
                panic!("unexpected early end of stream")
            }
            other => panic!("unexpected other output: {other:?}"),
        }
    }

    // Followed by List(None) to end the stream
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(None)))) if id == stream_id => (),
        Some(other) => panic!("expected list end, unexpected output: {other:?}"),
        None => panic!("missing list stream end signal"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_external_stream_stdout_only() {
    let test = TestCase::new();

    let stdout_chunks = vec![b"nushel".to_vec(), b"l rock".to_vec(), b"s!\n".to_vec()];

    let stdout_raw_stream = RawStream::new(
        Box::new(stdout_chunks.clone().into_iter().map(Ok)),
        None,
        Span::test_data(),
        None,
    );

    let span = Span::new(1000, 1050);

    let data = PipelineData::ExternalStream {
        stdout: Some(stdout_raw_stream),
        stderr: None,
        exit_code: None,
        span,
        metadata: None,
        trim_end_newline: false,
    };

    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write external stream pipeline data");

    // First, there should be a header telling us metadata about the external stream
    let id = match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::ExternalStream(info),
        ))) => {
            assert_eq!(span, info.span, "info.span");
            assert!(info.stderr.is_none(), "info.stderr: {:?}", info.stderr);
            assert!(
                info.exit_code.is_none(),
                "info.exit_code: {:?}",
                info.exit_code
            );
            assert!(!info.trim_end_newline, "info.trim_end_newline=true");
            match info.stdout {
                Some(RawStreamInfo {
                    id,
                    is_binary,
                    known_size,
                }) => {
                    let _ = is_binary; // undefined, could be anything
                    assert_eq!(None, known_size);
                    id
                }
                None => panic!("stdout missing"),
            }
        }
        Some(other) => panic!("unexpected response written: {other:?}"),
        None => panic!("no response written"),
    };

    // Then, just check the outputs. They should be in exactly the same order with nothing extra
    for expected_chunk in stdout_chunks {
        match test.next_written_output() {
            Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::Raw(option))))
                if id == stream_id =>
            {
                let read_chunk = option
                    .transpose()
                    .expect("error in stdout stream")
                    .expect("early EOF signal in stdout stream");
                assert_eq!(expected_chunk, read_chunk);
            }
            Some(other) => panic!("unexpected output: {other:?}"),
            None => panic!("unexpected end of output"),
        }
    }

    // And there should be an end of stream signal (`Ok(None)`)
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::Raw(option)))) if id == stream_id => {
            match option {
                Some(Ok(data)) => panic!("unexpected extra data on stdout stream: {data:?}"),
                Some(Err(err)) => panic!("unexpected error at end of stdout stream: {err}"),
                None => (),
            }
        }
        Some(other) => panic!("unexpected output: {other:?}"),
        None => panic!("unexpected end of output"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_external_stream_stdout_err() {
    let test = TestCase::new();

    let spec_msg = "something bad";
    let spec_val_span = Span::new(1090, 1100);
    let spec_call_span = Span::new(1000, 1030);

    let error = ShellError::IncorrectValue {
        msg: spec_msg.into(),
        val_span: spec_val_span,
        call_span: spec_call_span,
    };

    let stdout_raw_stream = RawStream::new(
        Box::new(std::iter::once(Err(error))),
        None,
        Span::test_data(),
        None,
    );

    let data = PipelineData::ExternalStream {
        stdout: Some(stdout_raw_stream),
        stderr: None,
        exit_code: None,
        span: Span::test_data(),
        metadata: None,
        trim_end_newline: false,
    };

    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write external stream pipeline data");

    // Check response header
    let id = match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::ExternalStream(info),
        ))) => {
            assert!(info.stdout.is_some(), "info.stdout is not present");
            assert!(info.stderr.is_none(), "info.stderr: {:?}", info.stderr);
            assert!(
                info.exit_code.is_none(),
                "info.exit_code: {:?}",
                info.exit_code
            );
            info.stdout.as_ref().unwrap().id
        }
        Some(other) => panic!("unexpected response written: {other:?}"),
        None => panic!("no response written"),
    };

    // Check error
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::Raw(Some(result)))))
            if id == stream_id =>
        {
            match result {
                Ok(value) => panic!("unexpected value in stream: {value:?}"),
                Err(ShellError::IncorrectValue {
                    msg,
                    val_span,
                    call_span,
                }) => {
                    assert_eq!(spec_msg, msg, "msg");
                    assert_eq!(spec_val_span, val_span, "val_span");
                    assert_eq!(spec_call_span, call_span, "call_span");
                }
                Err(err) => panic!("unexpected other error on stream: {err}"),
            }
        }
        Some(other) => panic!("unexpected output: {other:?}"),
        None => panic!("didn't write the exit code"),
    }

    // Check end of stream
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::Raw(None)))) if id == stream_id => (),
        Some(other) => panic!("unexpected output: {other:?}"),
        None => panic!("didn't write the exit code end of stream signal"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_external_stream_exit_code_only() {
    let test = TestCase::new();

    let exit_code_stream = ListStream::from_stream(std::iter::once(Value::test_int(0)), None);

    let data = PipelineData::ExternalStream {
        stdout: None,
        stderr: None,
        exit_code: Some(exit_code_stream),
        span: Span::test_data(),
        metadata: None,
        trim_end_newline: false,
    };

    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write external stream pipeline data");

    // Check response header
    let id = match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::ExternalStream(info),
        ))) => {
            // just check what matters here, the other tests cover other bits
            assert!(info.stdout.is_none(), "info.stdout: {:?}", info.stdout);
            assert!(info.stderr.is_none(), "info.stderr: {:?}", info.stderr);
            info.exit_code.as_ref().expect("info.exit_code missing").id
        }
        Some(other) => panic!("unexpected response: {other:?}"),
        None => panic!("didn't write any response"),
    };

    // Check exit code value
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(Some(value)))))
            if id == stream_id =>
        {
            assert_eq!(Value::test_int(0), value);
        }
        Some(other) => panic!("unexpected output: {other:?}"),
        None => panic!("didn't write the exit code"),
    }

    // Check end of stream
    match test.next_written_output() {
        Some(PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(None)))) if id == stream_id => (),
        Some(other) => panic!("unexpected output: {other:?}"),
        None => panic!("didn't write the exit code end of stream signal"),
    }

    assert!(!test.has_unconsumed_write());
}

#[test]
fn write_pipeline_data_response_external_stream_full() {
    let test = TestCase::new();

    // Consume three streams simultaneously. Can't predict which order the output will really be
    // in, though
    let stdout_chunks = vec![
        b"hello ".to_vec(),
        b"world\n".to_vec(),
        b"test".to_vec(),
        b" stdout".to_vec(),
    ];

    let stderr_chunks = vec![b"standard ".to_vec(), b"error\n".to_vec()];

    let exit_code_values = vec![
        // There probably wouldn't be more than one exit code normally, but try just in case...
        Value::test_int(0),
        Value::test_int(1),
        Value::test_int(2),
    ];

    let stdout_len = stdout_chunks.iter().map(|c| c.len()).sum::<usize>() as u64;

    let stdout_raw_stream = RawStream::new(
        Box::new(stdout_chunks.clone().into_iter().map(Ok)),
        None,
        Span::test_data(),
        Some(stdout_len),
    );
    let stderr_raw_stream = RawStream::new(
        Box::new(stderr_chunks.clone().into_iter().map(Ok)),
        None,
        Span::test_data(),
        None,
    );
    let exit_code_stream = ListStream::from_stream(exit_code_values.clone().into_iter(), None);

    let data = PipelineData::ExternalStream {
        stdout: Some(stdout_raw_stream),
        stderr: Some(stderr_raw_stream),
        exit_code: Some(exit_code_stream),
        span: Span::test_data(),
        metadata: None,
        trim_end_newline: true,
    };

    test.engine_interface()
        .write_pipeline_data_response(data)
        .expect("failed to write external stream pipeline data");

    // First, there should be a header telling us metadata about the external stream
    let (out, err, exit);
    match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::PipelineData(
            PipelineDataHeader::ExternalStream(info),
        ))) => {
            assert_eq!(Span::test_data(), info.span, "info.span");
            match info.stdout {
                Some(RawStreamInfo {
                    id,
                    is_binary,
                    known_size,
                }) => {
                    out = id;
                    let _ = is_binary; // undefined, could be anything
                    assert_eq!(Some(stdout_len), known_size);
                }
                None => panic!("stdout missing"),
            }
            match info.stderr {
                Some(RawStreamInfo {
                    id,
                    is_binary,
                    known_size,
                }) => {
                    err = id;
                    let _ = is_binary; // undefined, could be anything
                    assert_eq!(None, known_size);
                }
                None => panic!("stderr missing"),
            }
            exit = info.exit_code.as_ref().expect("exit_code missing").id;
            assert!(info.trim_end_newline);
        }
        Some(other) => panic!("unexpected response written: {other:?}"),
        None => panic!("no response written"),
    };

    // Then comes the hard part: check for the StreamData responses matching each of the iterators
    //
    // Each stream should be in order, but the order of the responses of unrelated streams is
    // not defined and may be random
    let mut stdout_iter = stdout_chunks.into_iter();
    let mut stderr_iter = stderr_chunks.into_iter();
    let mut exit_code_iter = exit_code_values.into_iter();

    for output in test.written_outputs() {
        match output {
            PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::List(value))) => {
                if stream_id == exit {
                    assert_eq!(exit_code_iter.next(), value);
                } else {
                    panic!("received unexpected list data on stream {stream_id}: {value:?}");
                }
            }
            PluginOutput::Stream(StreamMessage::Data(stream_id, StreamData::Raw(option))) => {
                if stream_id == out {
                    let received = option
                        .transpose()
                        .expect("unexpected error in stdout stream");
                    assert_eq!(stdout_iter.next(), received);
                } else if stream_id == err {
                    let received = option
                        .transpose()
                        .expect("unexpected error in stderr stream");
                    assert_eq!(stderr_iter.next(), received);
                } else {
                    panic!("received unexpected raw data on stream {stream_id}: {option:?}");
                }
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    // Make sure we got all of the messages we expected, and nothing extra
    assert!(
        stdout_iter.next().is_none(),
        "didn't match all stdout messages"
    );
    assert!(
        stderr_iter.next().is_none(),
        "didn't match all stderr messages"
    );
    assert!(
        exit_code_iter.next().is_none(),
        "didn't match all exit code messages"
    );

    assert!(!test.has_unconsumed_write());
}
