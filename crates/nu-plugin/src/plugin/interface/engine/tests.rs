use nu_protocol::{Span, Value, ShellError, PipelineData, CustomValue};
use serde::{Serialize, Deserialize};

use crate::{StreamData, PluginCallResponse};
use crate::plugin::interface::engine::EngineInterfaceIo;
use crate::plugin::interface::stream_data_io::{
    StreamDataIo, StreamBuffers, gen_stream_data_tests, StreamBuffer
};
use crate::plugin::interface::test_util::TestCase;
use crate::protocol::{
    PluginInput, PluginOutput, PluginCall, PluginData, CallInfo, EvaluatedCall, CallInput,
    ExternalStreamInfo, RawStreamInfo
};

use super::EngineInterface;

gen_stream_data_tests!(
    PluginInput (add_input),
    PluginOutput (next_written_output),
    |test| test.engine_interface_impl()
);

#[test]
fn read_call_signature() {
    let test = TestCase::new();
    test.add_input(PluginInput::Call(PluginCall::Signature));

    match test.engine_interface().read_call().unwrap() {
        Some(PluginCall::Signature) => (),
        Some(other) => panic!("read unexpected call: {:?}", other),
        None => panic!("end of input")
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
        input: CallInput::Empty,
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
        None => panic!("end of input")
    }
}

#[test]
fn read_call_collapse_custom_value() {
    let test = TestCase::new();
    let data = PluginData { data: vec![42, 13, 37], span: Span::test_data() };
    test.add_input(PluginInput::Call(PluginCall::CollapseCustomValue(data.clone())));

    match test.engine_interface().read_call().unwrap() {
        Some(PluginCall::CollapseCustomValue(read_data)) => assert_eq!(data, read_data),
        Some(other) => panic!("read unexpected call: {:?}", other),
        None => panic!("end of input")
    }
}

#[test]
fn read_call_unexpected_stream_data() {
    let test = TestCase::new();
    test.add_input(PluginInput::StreamData(StreamData::List(None)));
    test.add_input(PluginInput::Call(PluginCall::Signature));

    test.engine_interface().read_call().expect_err("should be an error");
}

#[test]
fn read_call_ignore_dropped_stream_data() {
    let test = TestCase::new();
    test.add_input(PluginInput::StreamData(StreamData::List(None)));
    test.add_input(PluginInput::Call(PluginCall::Signature));

    let interface = test.engine_interface_impl();
    interface.read.lock().unwrap().1.list = StreamBuffer::Dropped;
    interface.read_call().expect("should succeed");
}

fn test_call_with_input(input: CallInput) -> CallInfo {
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

fn dbg<T>(val: T) -> String where T: std::fmt::Debug {
    format!("{:?}", val)
}

fn validate_stream_data_acceptance(input: CallInput, accepts: [bool; 4]) {
    let test = TestCase::new();
    let call_info = test_call_with_input(input);
    test.add_input(PluginInput::Call(PluginCall::Run(call_info)));

    let interface = test.engine_interface_impl();

    interface.read_call().expect("call failed");

    let data_types = [
        StreamData::List(Some(Value::test_bool(true))),
        StreamData::ExternalStdout(Some(Ok(vec![]))),
        StreamData::ExternalStderr(Some(Ok(vec![]))),
        StreamData::ExternalExitCode(Some(Value::test_int(1)))
    ];

    for (data, accept) in data_types.iter().zip(accepts) {
        test.clear_input();
        test.add_input(PluginInput::StreamData(data.clone()));
        let result = match data {
            StreamData::List(_) => interface.read_list().map(dbg),
            StreamData::ExternalStdout(_) => interface.read_external_stdout().map(dbg),
            StreamData::ExternalStderr(_) => interface.read_external_stderr().map(dbg),
            StreamData::ExternalExitCode(_) => interface.read_external_exit_code().map(dbg),
        };
        match result {
            Ok(success) if !accept =>
                panic!("{data:?} was successfully consumed, but shouldn't have been: {success}"),
            Err(err) if accept =>
                panic!("{data:?} was rejected, but should have been accepted: {err}"),
            _ => ()
        }
    }
}

#[test]
fn read_call_run_with_empty_input_doesnt_accept_stream_data() {
    validate_stream_data_acceptance(CallInput::Empty, [false; 4])
}

#[test]
fn read_call_run_with_value_input_doesnt_accept_stream_data() {
    validate_stream_data_acceptance(CallInput::Value(Value::test_int(4)), [false; 4])
}

#[test]
fn read_call_run_with_list_stream_input_accepts_only_list_stream_data() {
    validate_stream_data_acceptance(CallInput::ListStream, [
        true, // list stream
        false,
        false,
        false
    ])
}

#[test]
fn read_call_run_with_external_stream_stdout_input_accepts_only_external_stream_stdout_data() {
    let call_input = CallInput::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo { is_binary: false, known_size: None }),
        stderr: None,
        has_exit_code: false,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(call_input, [
        false,
        true, // external stdout
        false,
        false
    ])
}

#[test]
fn read_call_run_with_external_stream_stderr_input_accepts_only_external_stream_stderr_data() {
    let call_input = CallInput::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: None,
        stderr: Some(RawStreamInfo { is_binary: false, known_size: None }),
        has_exit_code: false,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(call_input, [
        false,
        false,
        true, // external stderr
        false
    ])
}

#[test]
fn read_call_run_with_external_stream_exit_code_input_accepts_only_external_stream_exit_code_data() {
    let call_input = CallInput::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: None,
        stderr: None,
        has_exit_code: true,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(call_input, [
        false,
        false,
        false,
        true // external exit code
    ])
}

#[test]
fn read_call_run_with_external_stream_all_input_accepts_only_all_external_stream_data() {
    let call_input = CallInput::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo { is_binary: false, known_size: None }),
        stderr: Some(RawStreamInfo { is_binary: false, known_size: None }),
        has_exit_code: true,
        trim_end_newline: false,
    });

    validate_stream_data_acceptance(call_input, [
        false,
        true, // external stdout
        true, // external stderr
        true // external exit code
    ])
}

#[test]
fn read_call_end_of_input() {
    let test = TestCase::new();
    match test.engine_interface().read_call().expect("should succeed") {
        Some(call) => panic!("should have been end of input, but read {call:?}"),
        None => ()
    }
}

#[test]
fn read_call_io_error() {
    let test = TestCase::new();
    test.set_read_error(ShellError::IOError { msg: "test error".into() });

    match test.engine_interface().read_call().expect_err("should be an error") {
        ShellError::IOError { msg } if msg == "test error" => (),
        other => panic!("got some other error: {other}")
    }
}

#[test]
fn write_call_response() {
    let test = TestCase::new();
    let response = PluginCallResponse::Empty;
    test.engine_interface().write_call_response(response.clone()).expect("should succeed");
    match test.next_written_output() {
        Some(PluginOutput::CallResponse(PluginCallResponse::Empty)) => (),
        Some(other) => panic!("wrote the wrong message: {other:?}"),
        None => panic!("didn't write anything")
    }
}

#[test]
fn write_call_response_error() {
    let test = TestCase::new();
    test.set_write_error(ShellError::IOError { msg: "test error".into() });

    let response = PluginCallResponse::Empty;
    match test.engine_interface().write_call_response(response).expect_err("should be an error") {
        ShellError::IOError { msg } if msg == "test error" => (),
        other => panic!("got some other error: {other}")
    }
}

#[test]
fn make_pipeline_data_empty() {
    let test = TestCase::new();

    let pipe = test.engine_interface().make_pipeline_data(CallInput::Empty)
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
    let pipe = test.engine_interface().make_pipeline_data(CallInput::Value(value.clone()))
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

    fn to_base_value(&self, span: Span) -> Result<Value, ShellError>  {
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
        data: bincoded,
        span: Span::test_data(),
    };
    let call_input = CallInput::Data(data);

    let pipe = test.engine_interface().make_pipeline_data(call_input)
        .expect("failed to make pipeline data");

    match pipe {
        PipelineData::Empty => panic!("got empty, expected value"),
        PipelineData::Value(v, _) => {
            let read_custom = v.as_custom_value().expect("not a custom value");
            let read_downcast: &MyCustom =
                read_custom.as_any().downcast_ref().expect("wrong type");
            assert_eq!(&MyCustom(42), read_downcast);
        }
        PipelineData::ListStream(_, _) => panic!("got list stream"),
        PipelineData::ExternalStream { .. } => panic!("got external stream"),
    }
}

#[test]
fn make_pipeline_data_list_stream() {
    let test = TestCase::new();

    let values = [
        Value::test_int(4),
        Value::test_string("hello"),
    ];

    for value in &values {
        test.add_input(PluginInput::StreamData(StreamData::List(Some(value.clone()))));
    }
    // end
    test.add_input(PluginInput::StreamData(StreamData::List(None)));

    let call_input = CallInput::ListStream;

    let interface = EngineInterface::from({
        let interface = test.engine_interface_impl();
        interface.read.lock().unwrap().1 = StreamBuffers::new_list();
        interface
    });

    let pipe = interface.make_pipeline_data(call_input).expect("failed to make pipeline data");

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
        StreamData::ExternalStdout(Some(Ok(b"foo".to_vec()))),
        StreamData::ExternalStderr(Some(Ok(b"bar".to_vec()))),
        StreamData::ExternalExitCode(Some(Value::test_int(1))),
        StreamData::ExternalStderr(Some(Ok(b"barr".to_vec()))),
        StreamData::ExternalStderr(None),
        StreamData::ExternalStdout(Some(Ok(b"fooo".to_vec()))),
        StreamData::ExternalStdout(None),
        StreamData::ExternalExitCode(None),
    ];

    for data in stream_data {
        test.add_input(PluginInput::StreamData(data));
    }

    let call_input = CallInput::ExternalStream(ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo { is_binary: true, known_size: Some(7) }),
        stderr: Some(RawStreamInfo { is_binary: false, known_size: None }),
        has_exit_code: true,
        trim_end_newline: false,
    });

    let interface = EngineInterface::from({
        let interface = test.engine_interface_impl();
        interface.read.lock().unwrap().1 = StreamBuffers::new_external(true, true, true);
        interface
    });

    let pipe = interface.make_pipeline_data(call_input).expect("failed to make pipeline data");

    match pipe {
        PipelineData::ExternalStream {
            stdout, stderr, exit_code, span, trim_end_newline, ..
        } => {
            assert!(stdout.is_some());
            assert!(stderr.is_some());
            assert!(exit_code.is_some());
            assert_eq!(Span::test_data(), span, "span");
            assert_eq!(false, trim_end_newline, "trim_end_newline");

            if let Some(rs) = stdout.as_ref() {
                assert_eq!(true, rs.is_binary, "stdout.is_binary");
                assert_eq!(Some(7), rs.known_size, "stdout.known_size");
            }
            if let Some(rs) = stderr.as_ref() {
                assert_eq!(false, rs.is_binary, "stderr.is_binary");
                assert_eq!(None, rs.known_size, "stderr.known_size");
            }

            let out_bytes = stdout.unwrap().into_bytes().expect("failed to read stdout");
            let err_bytes = stderr.unwrap().into_bytes().expect("failed to read stderr");
            let exit_code_vals: Vec<_> = exit_code.unwrap().into_iter().collect();

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
#[ignore = "TODO"]
fn write_pipeline_data_response_empty() {
}

#[test]
#[ignore = "TODO"]
fn write_pipeline_data_response_value() {
}

#[test]
#[ignore = "TODO"]
fn write_pipeline_data_response_list_stream() {
}

#[test]
#[ignore = "TODO"]
fn write_pipeline_data_response_external_stream() {
}
