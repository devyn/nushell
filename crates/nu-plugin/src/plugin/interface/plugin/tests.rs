use std::path::Path;
use std::sync::Arc;

use crate::plugin::context::PluginExecutionContext;
use crate::plugin::interface::{
    buffers::PerStreamBuffers,
    stream_data_io::{def_streams, gen_stream_data_tests, StreamDataIo},
    test_util::TestCase,
    PluginInterface,
};
use crate::protocol::{
    CallInfo, ExternalStreamInfo, PipelineDataHeader, PluginCall, PluginCallResponse,
    PluginCustomValue, PluginData, PluginInput, PluginOutput, RawStreamInfo, StreamData, StreamId,
};

use super::PluginInterfaceIo;

use nu_protocol::engine::Closure;
use nu_protocol::{ListStream, PipelineData, ShellError, Span, Value, Spanned};

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

        Ok(PipelineData::Value(Value::test_list(
            vec![
                Value::closure(closure.item, closure.span),
                Value::test_list(positional),
                input.into_value(span),
                Value::test_bool(redirect_stdout),
                Value::test_bool(redirect_stderr),
            ]
        ), None))
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
    let id = 7;
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::ListStream(id)),
    ));

    let interface = test.plugin_interface_impl(None);

    let read_id = match interface.clone().read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::ListStream(id)) => id,
        other => panic!("read unexpected response: {:?}", other),
    };
    assert_eq!(id, read_id);

    {
        let mut read = interface.read.lock().unwrap();
        let bufs = read.stream_buffers.get(id).unwrap();
        assert!(matches!(bufs, PerStreamBuffers::List(..)));
    }
}

#[test]
fn read_call_response_external_stream() {
    let test = TestCase::new();
    let info = ExternalStreamInfo {
        span: Span::test_data(),
        stdout: Some(RawStreamInfo {
            is_binary: false,
            known_size: None,
        }),
        stderr: Some(RawStreamInfo {
            is_binary: false,
            known_size: None,
        }),
        has_exit_code: true,
        trim_end_newline: false,
    };
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::ExternalStream(0, info)),
    ));

    let interface = test.plugin_interface_impl(None);

    match interface.clone().read_call_response().unwrap() {
        PluginCallResponse::PipelineData(PipelineDataHeader::ExternalStream(read_id, _)) => {
            assert_eq!(0, read_id)
        }
        other => panic!("read unexpected response: {:?}", other),
    }

    {
        let mut read = interface.read.lock().unwrap();
        let buf = read.stream_buffers.get(0).unwrap();
        if let PerStreamBuffers::External {
            stdout,
            stderr,
            exit_code,
        } = buf
        {
            assert!(stdout.is_present());
            assert!(stderr.is_present());
            assert!(exit_code.is_present());
        } else {
            panic!("should be external");
        }
    }
}

#[test]
fn read_call_response_unexpected_stream_data() {
    let test = TestCase::new();
    test.add_output(PluginOutput::StreamData(0, StreamData::List(None)));
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(PipelineDataHeader::Empty),
    ));

    test.plugin_interface(None)
        .read_call_response()
        .expect_err("should be an error");
}

fn dbg<T>(val: T) -> String
where
    T: std::fmt::Debug,
{
    format!("{:?}", val)
}

fn validate_stream_data_acceptance(id: StreamId, header: PipelineDataHeader, accepts: [bool; 4]) {
    let test = TestCase::new();
    test.add_output(PluginOutput::CallResponse(
        PluginCallResponse::PipelineData(header),
    ));

    let interface = test.plugin_interface_impl(None);

    interface.clone().read_call_response().expect("call failed");

    let data_types = [
        StreamData::List(Some(Value::test_bool(true))),
        StreamData::ExternalStdout(Some(Ok(vec![]))),
        StreamData::ExternalStderr(Some(Ok(vec![]))),
        StreamData::ExternalExitCode(Some(Value::test_int(1))),
    ];

    for (data, accept) in data_types.iter().zip(accepts) {
        test.clear_output();
        test.add_output(PluginOutput::StreamData(id, data.clone()));
        let result = match data {
            StreamData::List(_) => interface.clone().read_list(id).map(dbg),
            StreamData::ExternalStdout(_) => interface.clone().read_external_stdout(id).map(dbg),
            StreamData::ExternalStderr(_) => interface.clone().read_external_stderr(id).map(dbg),
            StreamData::ExternalExitCode(_) => {
                interface.clone().read_external_exit_code(id).map(dbg)
            }
        };
        match result {
            Ok(success) if !accept => {
                panic!("{data:?} was successfully consumed, but shouldn't have been: {success}")
            }
            Err(err) if accept => {
                panic!("{data:?} was rejected, but should have been accepted: {err}")
            }
            _ => (),
        }
    }
}

#[test]
fn read_call_response_empty_doesnt_accept_stream_data() {
    validate_stream_data_acceptance(0, PipelineDataHeader::Empty, [false; 4])
}

#[test]
fn read_call_response_value_doesnt_accept_stream_data() {
    validate_stream_data_acceptance(
        1,
        PipelineDataHeader::Value(Value::test_int(4).into()),
        [false; 4],
    )
}

#[test]
fn read_call_response_list_stream_accepts_only_list_stream_data() {
    validate_stream_data_acceptance(
        2,
        PipelineDataHeader::ListStream(2),
        [
            true, // list stream
            false, false, false,
        ],
    )
}

#[test]
fn read_call_response_external_stream_stdout_accepts_only_external_stream_stdout_data() {
    let response = PipelineDataHeader::ExternalStream(
        3,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            stderr: None,
            has_exit_code: false,
            trim_end_newline: false,
        },
    );

    validate_stream_data_acceptance(
        3,
        response,
        [
            false, true, // external stdout
            false, false,
        ],
    )
}

#[test]
fn read_call_response_run_with_external_stream_stderr_input_accepts_only_external_stream_stderr_data(
) {
    let response = PipelineDataHeader::ExternalStream(
        4,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: None,
            stderr: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            has_exit_code: false,
            trim_end_newline: false,
        },
    );

    validate_stream_data_acceptance(
        4,
        response,
        [
            false, false, true, // external stderr
            false,
        ],
    )
}

#[test]
fn read_call_response_external_stream_exit_code_accepts_only_external_stream_exit_code_data() {
    let response = PipelineDataHeader::ExternalStream(
        5,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: None,
            stderr: None,
            has_exit_code: true,
            trim_end_newline: false,
        },
    );

    validate_stream_data_acceptance(
        5,
        response,
        [
            false, false, false, true, // external exit code
        ],
    )
}

#[test]
fn read_call_response_external_stream_all_accepts_only_all_external_stream_data() {
    let response = PipelineDataHeader::ExternalStream(
        6,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            stderr: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            has_exit_code: true,
            trim_end_newline: false,
        },
    );

    validate_stream_data_acceptance(
        6,
        response,
        [
            false, true, // external stdout
            true, // external stderr
            true, // external exit code
        ],
    )
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
        test.add_output(PluginOutput::StreamData(
            0,
            StreamData::List(Some(value.clone())),
        ));
    }
    // end
    test.add_output(PluginOutput::StreamData(0, StreamData::List(None)));

    let header = PipelineDataHeader::ListStream(0);

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        def_streams!(interface, list(0));
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
        test.add_output(PluginOutput::StreamData(0, data));
    }

    let header = PipelineDataHeader::ExternalStream(
        0,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: Some(RawStreamInfo {
                is_binary: true,
                known_size: Some(7),
            }),
            stderr: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            has_exit_code: true,
            trim_end_newline: false,
        },
    );

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        def_streams!(interface, external(0));
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
        StreamData::ExternalExitCode(Some(Value::int(1, Span::test_data()))),
        StreamData::ExternalStdout(Some(Err(ShellError::NushellFailed {
            msg: spec_msg.into(),
        }))),
        StreamData::ExternalStdout(None),
    ];

    let id = 42;

    for data in stream_data {
        test.add_output(PluginOutput::StreamData(id, data));
    }

    // Still enable the other streams, to ensure ignoring the other data works
    let header = PipelineDataHeader::ExternalStream(
        id,
        ExternalStreamInfo {
            span: Span::test_data(),
            stdout: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            stderr: Some(RawStreamInfo {
                is_binary: false,
                known_size: None,
            }),
            has_exit_code: true,
            trim_end_newline: false,
        },
    );

    let interface = PluginInterface::from({
        let interface = test.plugin_interface_impl(Some(Arc::new(BogusContext)));
        def_streams!(interface, external(id));
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

    let header = PipelineDataHeader::ListStream(0);

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
