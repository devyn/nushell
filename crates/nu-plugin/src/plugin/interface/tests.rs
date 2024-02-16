use std::sync::{atomic::AtomicBool, Arc, Mutex};

use nu_protocol::{Value, ShellError, PipelineData, Span};

use crate::{protocol::{PluginData, PluginInput, PluginOutput, PipelineDataHeader, StreamMessage, ListStreamInfo, ExternalStreamInfo, RawStreamInfo}, sequence::Sequence, plugin::interface::PluginRead};

use super::{PluginWrite, stream::{StreamManager, StreamManagerHandle}, test_util::{TestCase, TestData}, InterfaceManager, Interface};

#[derive(Debug)]
struct TestInterfaceManager {
    stream_manager: StreamManager,
    writer: Arc<Mutex<TestData<PluginOutput>>>,
    seq: Arc<Sequence>,
}

#[derive(Debug, Clone)]
struct TestInterface {
    stream_manager_handle: StreamManagerHandle,
    writer: Arc<Mutex<TestData<PluginOutput>>>,
    seq: Arc<Sequence>,
}

impl TestInterfaceManager {
    fn new(test: &TestCase<PluginInput, PluginOutput>) -> TestInterfaceManager {
        TestInterfaceManager {
            stream_manager: StreamManager::new(),
            writer: test.out.clone(),
            seq: Arc::new(Sequence::default()),
        }
    }
}

impl InterfaceManager for TestInterfaceManager {
    type Interface = TestInterface;
    type Input = PluginInput;
    type Context = ();

    fn get_interface(&self) -> Self::Interface {
        TestInterface {
            stream_manager_handle: self.stream_manager.get_handle(),
            writer: self.writer.clone(),
            seq: self.seq.clone(),
        }
    }

    fn consume(&mut self, input: Self::Input) -> Result<(), ShellError> {
        match input {
            PluginInput::Stream(msg) => self.consume_stream_message(msg),
            _ => unimplemented!()
        }
    }

    fn value_from_plugin_data(
        &self,
        data: PluginData,
        _context: &(),
    ) -> Result<Value, ShellError> {
        Ok(Value::binary(data.data, data.span))
    }

    fn ctrlc(&self, _context: &Self::Context) -> Option<Arc<AtomicBool>> {
        None
    }

    fn stream_manager(&self) -> &StreamManager {
        &self.stream_manager
    }
}

impl Interface for TestInterface {
    type Output = PluginOutput;
    type Context = ();

    fn write(&self, output: Self::Output) -> Result<(), ShellError> {
        self.writer.write(&output)
    }

    fn flush(&self) -> Result<(), ShellError> {
        Ok(())
    }

    fn stream_id_sequence(&self) -> &Sequence {
        &self.seq
    }

    fn stream_manager_handle(&self) -> &StreamManagerHandle {
        &self.stream_manager_handle
    }

    fn value_to_plugin_data(
        &self,
        value: &Value,
        _context: &(),
    ) -> Result<Option<PluginData>, ShellError> {
        Ok(match value {
            Value::Binary { val, .. } => Some(PluginData {
                name: None,
                data: val.clone(),
                span: value.span(),
            }),
            _ => None,
        })
    }
}

#[test]
fn read_pipeline_data_empty() -> Result<(), ShellError> {
    let manager = TestInterfaceManager::new(&TestCase::new());
    let header = PipelineDataHeader::Empty;

    assert!(matches!(manager.read_pipeline_data(header, &())?, PipelineData::Empty));
    Ok(())
}

#[test]
fn read_pipeline_data_value() -> Result<(), ShellError> {
    let manager = TestInterfaceManager::new(&TestCase::new());
    let value = Value::test_int(4);
    let header = PipelineDataHeader::Value(value.clone());

    match manager.read_pipeline_data(header, &())? {
        PipelineData::Value(read_value, _) => assert_eq!(value, read_value),
        PipelineData::ListStream(_, _) => panic!("unexpected ListStream"),
        PipelineData::ExternalStream { .. } => panic!("unexpected ExternalStream"),
        PipelineData::Empty => panic!("unexpected Empty"),
    }

    Ok(())
}

#[test]
fn read_pipeline_data_plugin_data() -> Result<(), ShellError> {
    let manager = TestInterfaceManager::new(&TestCase::new());
    let data = PluginData {
        name: None,
        data: vec![4, 5, 6, 7],
        span: Span::new(4, 10),
    };
    // Our implementation in TestInterfaceManager does this:
    let expected_value = Value::binary(data.data.clone(), data.span);
    let header = PipelineDataHeader::PluginData(data);

    match manager.read_pipeline_data(header, &())? {
        PipelineData::Value(read_value, _) => assert_eq!(expected_value, read_value),
        PipelineData::ListStream(_, _) => panic!("unexpected ListStream"),
        PipelineData::ExternalStream { .. } => panic!("unexpected ExternalStream"),
        PipelineData::Empty => panic!("unexpected Empty"),
    }

    Ok(())
}

#[test]
fn read_pipeline_data_list_stream() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = TestInterfaceManager::new(&test);

    let data = (0..100).map(Value::test_int).collect::<Vec<_>>();

    for value in &data {
        test.add(StreamMessage::Data(7, value.clone().into()));
    }
    test.add(StreamMessage::End(7));

    let header = PipelineDataHeader::ListStream(ListStreamInfo { id: 7 });

    let pipe = manager.read_pipeline_data(header, &())?;
    assert!(matches!(pipe, PipelineData::ListStream(..)), "unexpected PipelineData: {pipe:?}");

    // need to consume input
    while let Some(msg) = test.r#in.read()? {
        manager.consume(msg)?;
    }

    let mut count = 0;
    for (expected, read) in data.into_iter().zip(pipe) {
        assert_eq!(expected, read);
        count += 1;
    }
    assert_eq!(100, count);

    assert!(test.has_unconsumed_write());

    Ok(())
}

#[test]
fn read_pipeline_data_external_stream() -> Result<(), ShellError> {
    let mut test = TestCase::new();
    let mut manager = TestInterfaceManager::new(&test);

    let iterations = 100;
    let out_pattern = b"hello".to_vec();
    let err_pattern = vec![5, 4, 3, 2];

    test.add(StreamMessage::Data(14, Value::test_int(1).into()));
    for _ in 0..iterations {
        test.add(StreamMessage::Data(12, Ok(out_pattern.clone()).into()));
        test.add(StreamMessage::Data(13, Ok(err_pattern.clone()).into()));
    }
    test.add(StreamMessage::End(12));
    test.add(StreamMessage::End(13));
    test.add(StreamMessage::End(14));

    let test_span = Span::new(10, 13);
    let header = PipelineDataHeader::ExternalStream(ExternalStreamInfo {
        span: test_span,
        stdout: Some(RawStreamInfo {
            id: 12,
            is_binary: false,
            known_size: Some((out_pattern.len() * iterations) as u64),
        }),
        stderr: Some(RawStreamInfo {
            id: 13,
            is_binary: true,
            known_size: None,
        }),
        exit_code: Some(ListStreamInfo { id: 14 }),
        trim_end_newline: true,
    });

    let pipe = manager.read_pipeline_data(header, &())?;

    // need to consume input
    while let Some(msg) = test.r#in.read()? {
        manager.consume(msg)?;
    }

    match pipe {
        PipelineData::ExternalStream { stdout, stderr, exit_code, span, metadata, trim_end_newline } => {
            let stdout = stdout.expect("stdout is None");
            let stderr = stderr.expect("stderr is None");
            let exit_code = exit_code.expect("exit_code is None");
            assert_eq!(test_span, span);
            assert!(metadata.is_none());
            assert!(trim_end_newline);

            assert!(!stdout.is_binary);
            assert!(stderr.is_binary);

            assert_eq!(Some((out_pattern.len() * iterations) as u64), stdout.known_size);
            assert_eq!(None, stderr.known_size);

            // check the streams
            let mut count = 0;
            for chunk in stdout.stream {
                assert_eq!(out_pattern, chunk?);
                count += 1;
            }
            assert_eq!(iterations, count, "stdout length");
            let mut count = 0;

            for chunk in stderr.stream {
                assert_eq!(err_pattern, chunk?);
                count += 1;
            }
            assert_eq!(iterations, count, "stderr length");

            assert_eq!(vec![Value::test_int(1)], exit_code.collect::<Vec<_>>());
        }
        _ => panic!("unexpected PipelineData: {pipe:?}"),
    }

    // Don't need to check exactly what was written, just be sure that there is some output
    assert!(test.has_unconsumed_write());

    Ok(())
}
