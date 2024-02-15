macro_rules! generate_tests {
    ($encoder:expr) => {
        use crate::protocol::{
            CallInfo, EvaluatedCall, LabeledError, PipelineDataHeader, PluginCall,
            PluginCallResponse, PluginData, PluginInput, PluginOutput, StreamData, StreamMessage,
        };
        use nu_protocol::{PluginSignature, Span, Spanned, SyntaxShape, Value};

        #[test]
        fn decode_eof() {
            let mut buffer: &[u8] = &[];
            let encoder = $encoder;
            let result = encoder
                .decode_input(&mut buffer)
                .expect("eof should not result in an error");
            assert!(result.is_none(), "decode_input result: {result:?}");
            let result = encoder
                .decode_output(&mut buffer)
                .expect("eof should not result in an error");
            assert!(result.is_none(), "decode_output result: {result:?}");
        }

        #[test]
        fn decode_io_error() {
            struct ErrorProducer;
            impl std::io::Read for ErrorProducer {
                fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
                }
            }
            let encoder = $encoder;
            let mut buffered = std::io::BufReader::new(ErrorProducer);
            match encoder.decode_input(&mut buffered) {
                Ok(_) => panic!("decode_input: i/o error was not passed through"),
                Err(ShellError::IOError { .. }) => (), // okay
                Err(other) => panic!(
                    "decode_input: got other error, should have been a \
                    ShellError::IOError: {other:?}"
                ),
            }
            match encoder.decode_output(&mut buffered) {
                Ok(_) => panic!("decode_output: i/o error was not passed through"),
                Err(ShellError::IOError { .. }) => (), // okay
                Err(other) => panic!(
                    "decode_output: got other error, should have been a \
                    ShellError::IOError: {other:?}"
                ),
            }
        }

        #[test]
        fn decode_gibberish() {
            // just a sequence of bytes that shouldn't be valid in anything we use
            let gibberish: &[u8] = &[
                0, 80, 74, 85, 117, 122, 86, 100, 74, 115, 20, 104, 55, 98, 67, 203, 83, 85, 77,
                112, 74, 79, 254, 71, 80,
            ];
            let encoder = $encoder;

            let mut buffered = std::io::BufReader::new(&gibberish[..]);
            match encoder.decode_input(&mut buffered) {
                Ok(value) => panic!("decode_input: parsed successfully => {value:?}"),
                Err(ShellError::PluginFailedToDecode { .. }) => (), // okay
                Err(other) => panic!(
                    "decode_input: got other error, should have been a \
                    ShellError::PluginFailedToDecode: {other:?}"
                ),
            }

            let mut buffered = std::io::BufReader::new(&gibberish[..]);
            match encoder.decode_output(&mut buffered) {
                Ok(value) => panic!("decode_output: parsed successfully => {value:?}"),
                Err(ShellError::PluginFailedToDecode { .. }) => (), // okay
                Err(other) => panic!(
                    "decode_output: got other error, should have been a \
                    ShellError::PluginFailedToDecode: {other:?}"
                ),
            }
        }

        #[test]
        fn call_round_trip_signature() {
            let plugin_call = PluginCall::Signature;
            let plugin_input = PluginInput::Call(0, plugin_call);
            let encoder = $encoder;

            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_input(&plugin_input, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_input(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginInput::Call(0, PluginCall::Signature) => {}
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn call_round_trip_run() {
            let name = "test".to_string();

            let input = Value::bool(false, Span::new(1, 20));

            let call = EvaluatedCall {
                head: Span::new(0, 10),
                positional: vec![
                    Value::float(1.0, Span::new(0, 10)),
                    Value::string("something", Span::new(0, 10)),
                ],
                named: vec![(
                    Spanned {
                        item: "name".to_string(),
                        span: Span::new(0, 10),
                    },
                    Some(Value::float(1.0, Span::new(0, 10))),
                )],
            };

            let plugin_call = PluginCall::Run(CallInfo {
                name: name.clone(),
                call: call.clone(),
                input: PipelineDataHeader::Value(input.clone()),
                config: None,
            });

            let plugin_input = PluginInput::Call(1, plugin_call);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_input(&plugin_input, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_input(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginInput::Call(1, PluginCall::Run(call_info)) => {
                    assert_eq!(name, call_info.name);
                    assert_eq!(PipelineDataHeader::Value(input), call_info.input);
                    assert_eq!(call.head, call_info.call.head);
                    assert_eq!(call.positional.len(), call_info.call.positional.len());

                    call.positional
                        .iter()
                        .zip(call_info.call.positional.iter())
                        .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));

                    call.named
                        .iter()
                        .zip(call_info.call.named.iter())
                        .for_each(|(lhs, rhs)| {
                            // Comparing the keys
                            assert_eq!(lhs.0.item, rhs.0.item);

                            match (&lhs.1, &rhs.1) {
                                (None, None) => {}
                                (Some(a), Some(b)) => assert_eq!(a, b),
                                _ => panic!("not matching values"),
                            }
                        });
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn call_round_trip_collapsecustomvalue() {
            let data = vec![1, 2, 3, 4, 5, 6, 7];
            let span = Span::new(0, 20);

            let collapse_custom_value = PluginCall::CollapseCustomValue(PluginData {
                name: None,
                data: data.clone(),
                span,
            });

            let plugin_input = PluginInput::Call(2, collapse_custom_value);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_input(&plugin_input, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_input(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginInput::Call(2, PluginCall::CollapseCustomValue(plugin_data)) => {
                    assert!(plugin_data.name.is_none());
                    assert_eq!(data, plugin_data.data);
                    assert_eq!(span, plugin_data.span);
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn response_round_trip_signature() {
            let signature = PluginSignature::build("nu-plugin")
                .required("first", SyntaxShape::String, "first required")
                .required("second", SyntaxShape::Int, "second required")
                .required_named("first-named", SyntaxShape::String, "first named", Some('f'))
                .required_named(
                    "second-named",
                    SyntaxShape::String,
                    "second named",
                    Some('s'),
                )
                .rest("remaining", SyntaxShape::Int, "remaining");

            let response = PluginCallResponse::Signature(vec![signature.clone()]);
            let output = PluginOutput::CallResponse(3, response);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::CallResponse(
                    3,
                    PluginCallResponse::Signature(returned_signature),
                ) => {
                    assert_eq!(returned_signature.len(), 1);
                    assert_eq!(signature.sig.name, returned_signature[0].sig.name);
                    assert_eq!(signature.sig.usage, returned_signature[0].sig.usage);
                    assert_eq!(
                        signature.sig.extra_usage,
                        returned_signature[0].sig.extra_usage
                    );
                    assert_eq!(signature.sig.is_filter, returned_signature[0].sig.is_filter);

                    signature
                        .sig
                        .required_positional
                        .iter()
                        .zip(returned_signature[0].sig.required_positional.iter())
                        .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));

                    signature
                        .sig
                        .optional_positional
                        .iter()
                        .zip(returned_signature[0].sig.optional_positional.iter())
                        .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));

                    signature
                        .sig
                        .named
                        .iter()
                        .zip(returned_signature[0].sig.named.iter())
                        .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));

                    assert_eq!(
                        signature.sig.rest_positional,
                        returned_signature[0].sig.rest_positional,
                    );
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn response_round_trip_value() {
            let value = Value::int(10, Span::new(2, 30));

            let response = PluginCallResponse::value(value.clone());
            let output = PluginOutput::CallResponse(4, response);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::CallResponse(
                    4,
                    PluginCallResponse::PipelineData(PipelineDataHeader::Value(returned_value)),
                ) => {
                    assert_eq!(value, returned_value)
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn response_round_trip_plugin_data() {
            let name = Some("test".to_string());

            let data = vec![1, 2, 3, 4, 5];
            let span = Span::new(2, 30);

            let response =
                PluginCallResponse::PipelineData(PipelineDataHeader::PluginData(PluginData {
                    name: name.clone(),
                    data: data.clone(),
                    span,
                }));
            let output = PluginOutput::CallResponse(5, response);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::CallResponse(
                    5,
                    PluginCallResponse::PipelineData(PipelineDataHeader::PluginData(
                        returned_plugin_data,
                    )),
                ) => {
                    assert_eq!(name, returned_plugin_data.name);
                    assert_eq!(data, returned_plugin_data.data);
                    assert_eq!(span, returned_plugin_data.span);
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn response_round_trip_error() {
            let error = LabeledError {
                label: "label".into(),
                msg: "msg".into(),
                span: Some(Span::new(2, 30)),
            };
            let response = PluginCallResponse::Error(error.clone());
            let output = PluginOutput::CallResponse(6, response);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::CallResponse(6, PluginCallResponse::Error(msg)) => {
                    assert_eq!(error, msg)
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn response_round_trip_error_none() {
            let error = LabeledError {
                label: "label".into(),
                msg: "msg".into(),
                span: None,
            };
            let response = PluginCallResponse::Error(error.clone());
            let output = PluginOutput::CallResponse(7, response);

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::CallResponse(7, PluginCallResponse::Error(msg)) => {
                    assert_eq!(error, msg)
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn input_round_trip_stream_data_list() {
            let span = Span::new(12, 30);
            let item = Value::int(1, span);

            let stream_data = StreamData::List(item.clone());
            let plugin_input = PluginInput::Stream(StreamMessage::Data(0, stream_data));

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_input(&plugin_input, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_input(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginInput::Stream(StreamMessage::Data(id, StreamData::List(list_data))) => {
                    assert_eq!(0, id);
                    assert_eq!(item, list_data);
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn input_round_trip_stream_data_raw() {
            let data = b"Hello world";

            let stream_data = StreamData::Raw(Ok(data.to_vec()));
            let plugin_input = PluginInput::Stream(StreamMessage::Data(1, stream_data));

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_input(&plugin_input, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_input(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginInput::Stream(StreamMessage::Data(id, StreamData::Raw(bytes))) => {
                    assert_eq!(1, id);
                    match bytes {
                        Ok(bytes) => assert_eq!(data, &bytes[..]),
                        Err(err) => panic!("decoded into error variant: {err:?}"),
                    }
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn output_round_trip_stream_data_list() {
            let span = Span::new(12, 30);
            let item = Value::int(1, span);

            let stream_data = StreamData::List(item.clone());
            let plugin_output = PluginOutput::Stream(StreamMessage::Data(4, stream_data));

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&plugin_output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::Stream(StreamMessage::Data(id, StreamData::List(list_data))) => {
                    assert_eq!(4, id);
                    assert_eq!(item, list_data);
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }

        #[test]
        fn output_round_trip_stream_data_raw() {
            let data = b"Hello world";

            let stream_data = StreamData::Raw(Ok(data.to_vec()));
            let plugin_output = PluginOutput::Stream(StreamMessage::Data(5, stream_data));

            let encoder = $encoder;
            let mut buffer: Vec<u8> = Vec::new();
            encoder
                .encode_output(&plugin_output, &mut buffer)
                .expect("unable to serialize message");
            let returned = encoder
                .decode_output(&mut buffer.as_slice())
                .expect("unable to deserialize message")
                .expect("eof");

            match returned {
                PluginOutput::Stream(StreamMessage::Data(id, StreamData::Raw(bytes))) => {
                    assert_eq!(5, id);
                    match bytes {
                        Ok(bytes) => assert_eq!(data, &bytes[..]),
                        Err(err) => panic!("decoded into error variant: {err:?}"),
                    }
                }
                _ => panic!("decoded into wrong value: {returned:?}"),
            }
        }
    };
}

pub(crate) use generate_tests;
