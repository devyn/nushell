use std::{collections::VecDeque, sync::Arc};

use nu_protocol::{ShellError, Span, Value};

use crate::{
    protocol::{ExternalStreamInfo, PipelineDataHeader, PluginData, RawStreamInfo},
    StreamData,
};

use super::{PerStreamBuffers, StreamBuffer, StreamBuffers};

#[test]
fn stream_buffers_get_non_existing_is_err() {
    let mut buffers = StreamBuffers::default();
    buffers.get(0).expect_err("should be an error");
}

#[test]
fn stream_buffers_insert_existing_is_err() {
    let mut buffers = StreamBuffers {
        streams: vec![(0, Box::new(PerStreamBuffers::new_list()))],
    };
    buffers
        .insert(0, PerStreamBuffers::new_list())
        .expect_err("should be an error");
}

#[test]
fn stream_buffers_cleanup() {
    let mut buffers = StreamBuffers {
        streams: vec![
            (0, Box::new(PerStreamBuffers::new_list())),
            (
                1,
                Box::new(PerStreamBuffers::External {
                    stdout: StreamBuffer::NotPresent,
                    stderr: StreamBuffer::Present {
                        queue: VecDeque::new(),
                        ended: true,
                    },
                    exit_code: StreamBuffer::Dropped { ended: true },
                }),
            ),
            (
                2,
                Box::new(PerStreamBuffers::new_external(true, false, false)),
            ),
        ],
    };
    buffers.cleanup();
    assert!(buffers.get(0).is_ok(), "cleaned up the wrong stream");
    assert!(buffers.get(1).is_err(), "failed to clean up");
    assert!(buffers.get(2).is_ok(), "cleaned up the wrong stream");
}

#[test]
fn stream_buffers_init_stream_non_stream_header() -> Result<(), ShellError> {
    let mut buffers = StreamBuffers::default();
    buffers.init_stream(&PipelineDataHeader::Empty)?;
    assert!(
        buffers.streams.is_empty(),
        "stream was created erroneously for Empty"
    );
    buffers.init_stream(&PipelineDataHeader::Value(Value::test_bool(true)))?;
    assert!(
        buffers.streams.is_empty(),
        "stream was created erroneously for Value"
    );
    buffers.init_stream(&PipelineDataHeader::PluginData(PluginData {
        name: None,
        data: vec![],
        span: Span::test_data(),
    }))?;
    assert!(
        buffers.streams.is_empty(),
        "stream was created erroneously for PluginData"
    );
    Ok(())
}

#[test]
fn stream_buffers_init_stream_list() -> Result<(), ShellError> {
    let mut buffers = StreamBuffers::default();

    buffers.init_stream(&PipelineDataHeader::ListStream(4))?;

    match buffers.get(4)? {
        PerStreamBuffers::List(_) => (),
        PerStreamBuffers::External { .. } => panic!("init_stream created wrong type"),
    }
    Ok(())
}

#[test]
fn stream_buffers_init_stream_external() -> Result<(), ShellError> {
    let mut buffers = StreamBuffers::default();

    buffers.init_stream(&PipelineDataHeader::ExternalStream(
        1,
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
    ))?;

    match buffers.get(1)? {
        PerStreamBuffers::List(_) => panic!("init_stream created wrong type"),
        PerStreamBuffers::External {
            stdout,
            stderr,
            exit_code,
        } => {
            assert!(stdout.is_present());
            assert!(stderr.is_not_present());
            assert!(exit_code.is_not_present());
        }
    }
    Ok(())
}

#[test]
fn stream_buffers_skip() -> Result<(), ShellError> {
    let mut buffers = StreamBuffers {
        streams: vec![
            (1, Box::new(PerStreamBuffers::new_list())),
            (2, Box::new(PerStreamBuffers::new_list())),
        ],
    };
    buffers.skip(2, StreamData::List(Some(Value::test_int(4))))?;
    assert!(matches!(buffers.get(2)?.pop_list()?, Some(Some(_))));
    buffers.skip(1, StreamData::List(Some(Value::test_int(5))))?;
    assert!(matches!(buffers.get(1)?.pop_list()?, Some(Some(_))));

    buffers
        .skip(4, StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect_err("trying to write to a non-existent stream should have been an error");
    Ok(())
}

#[test]
fn per_stream_buffers_list_accepts_only_list_stream_data() {
    let mut buffers = PerStreamBuffers::new_list();

    buffers
        .push_back(StreamData::List(Some(Value::test_bool(true))))
        .expect("list was not accepted");

    buffers
        .push_back(StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect_err("external stdout was accepted");

    buffers
        .push_back(StreamData::ExternalStderr(Some(Ok(vec![]))))
        .expect_err("external stderr was accepted");

    buffers
        .push_back(StreamData::ExternalExitCode(Some(Value::test_int(1))))
        .expect_err("external exit code was accepted");
}

#[test]
fn stream_buffers_external_stream_stdout_accepts_only_external_stream_stdout_data() {
    let mut buffers = PerStreamBuffers::new_external(true, false, false);

    buffers
        .push_back(StreamData::List(Some(Value::test_bool(true))))
        .expect_err("list was accepted");

    buffers
        .push_back(StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect("external stdout was not accepted");

    buffers
        .push_back(StreamData::ExternalStderr(Some(Ok(vec![]))))
        .expect_err("external stderr was accepted");

    buffers
        .push_back(StreamData::ExternalExitCode(Some(Value::test_int(1))))
        .expect_err("external exit code was accepted");
}

#[test]
fn per_stream_buffers_external_stream_stderr_accepts_only_external_stream_stderr_data() {
    let mut buffers = PerStreamBuffers::new_external(false, true, false);

    buffers
        .push_back(StreamData::List(Some(Value::test_bool(true))))
        .expect_err("list was accepted");

    buffers
        .push_back(StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect_err("external stdout was accepted");

    buffers
        .push_back(StreamData::ExternalStderr(Some(Ok(vec![]))))
        .expect("external stderr was not accepted");

    buffers
        .push_back(StreamData::ExternalExitCode(Some(Value::test_int(1))))
        .expect_err("external exit code was accepted");
}

#[test]
fn per_stream_buffers_external_stream_exit_code_accepts_only_external_stream_exit_code_data() {
    let mut buffers = PerStreamBuffers::new_external(false, false, true);

    buffers
        .push_back(StreamData::List(Some(Value::test_bool(true))))
        .expect_err("list was accepted");

    buffers
        .push_back(StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect_err("external stdout was accepted");

    buffers
        .push_back(StreamData::ExternalStderr(Some(Ok(vec![]))))
        .expect_err("external stderr was accepted");

    buffers
        .push_back(StreamData::ExternalExitCode(Some(Value::test_int(1))))
        .expect("external exit code was not accepted");
}

#[test]
fn per_stream_buffers_external_stream_all_true_accepts_only_all_external_stream_data() {
    let mut buffers = PerStreamBuffers::new_external(true, true, true);

    buffers
        .push_back(StreamData::List(Some(Value::test_bool(true))))
        .expect_err("list was accepted");

    buffers
        .push_back(StreamData::ExternalStdout(Some(Ok(vec![]))))
        .expect("external stdout was not accepted");

    buffers
        .push_back(StreamData::ExternalStderr(Some(Ok(vec![]))))
        .expect("external stderr was not accepted");

    buffers
        .push_back(StreamData::ExternalExitCode(Some(Value::test_int(1))))
        .expect("external exit code was not accepted");
}

#[test]
fn per_stream_buffers_list_is_fully_consumed() -> Result<(), ShellError> {
    let mut buffers = PerStreamBuffers::new_list();
    assert!(!buffers.is_fully_consumed());
    buffers.push_back(StreamData::List(None))?;
    assert!(buffers.is_fully_consumed());
    Ok(())
}

#[test]
fn per_stream_buffers_external_all_is_fully_consumed() -> Result<(), ShellError> {
    let mut buffers = PerStreamBuffers::new_external(true, true, true);
    assert!(!buffers.is_fully_consumed(), "initial state");
    buffers.push_back(StreamData::ExternalStdout(None))?;
    assert!(!buffers.is_fully_consumed(), "after closing stdout");
    buffers.push_back(StreamData::ExternalStderr(None))?;
    assert!(!buffers.is_fully_consumed(), "after closing stderr");
    buffers.push_back(StreamData::ExternalExitCode(None))?;
    assert!(buffers.is_fully_consumed());
    Ok(())
}

#[test]
fn per_stream_buffers_external_stdout_is_fully_consumed() -> Result<(), ShellError> {
    let mut buffers = PerStreamBuffers::new_external(true, false, false);
    assert!(!buffers.is_fully_consumed());
    buffers.push_back(StreamData::ExternalStdout(None))?;
    assert!(buffers.is_fully_consumed());
    Ok(())
}

#[test]
fn stream_buffer_push_pop() -> Result<(), ShellError> {
    let mut buffer = StreamBuffer::present();
    buffer.push_back(Some(1))?;
    buffer.push_back(Some(2))?;
    assert_eq!(buffer.pop_front()?, Some(Some(1)));
    assert_eq!(buffer.pop_front()?, Some(Some(2)));
    assert_eq!(buffer.pop_front()?, None);
    buffer.push_back(Some(42))?;
    assert_eq!(buffer.pop_front()?, Some(Some(42)));
    assert_eq!(buffer.pop_front()?, None);
    buffer.push_back(None)?;
    assert_eq!(buffer.pop_front()?, Some(None));
    assert_eq!(buffer.pop_front()?, Some(None));
    Ok(())
}

#[test]
fn stream_buffer_write_after_end_err() -> Result<(), ShellError> {
    let mut buffer = StreamBuffer::present();
    buffer.push_back(Some(1))?;
    buffer.push_back(None)?;
    buffer
        .push_back(Some(2))
        .expect_err("write after end succeeded");
    Ok(())
}

#[test]
fn stream_buffer_is_fully_consumed() -> Result<(), ShellError> {
    let mut buffer = StreamBuffer::present();
    assert!(
        !buffer.is_fully_consumed(),
        "default state is fully consumed"
    );
    buffer.push_back(Some(1))?;
    assert!(
        !buffer.is_fully_consumed(),
        "fully consumed after pushing Some"
    );
    buffer.pop_front()?;
    assert!(!buffer.is_fully_consumed(), "fully consumed after popping");
    buffer.push_back(Some(1))?;
    buffer.push_back(None)?;
    assert!(
        !buffer.is_fully_consumed(),
        "fully consumed after pushing None"
    );
    buffer.pop_front()?;
    assert!(
        buffer.is_fully_consumed(),
        "not fully consumed after last message"
    );
    Ok(())
}

#[test]
fn stream_buffer_dropped_is_fully_consumed() {
    assert!(!StreamBuffer::<()>::Dropped { ended: false }.is_fully_consumed());
    assert!(StreamBuffer::<()>::Dropped { ended: true }.is_fully_consumed());
}

#[test]
fn stream_buffer_not_present_push_err() {
    StreamBuffer::NotPresent
        .push_back(Some(2))
        .expect_err("should be an error");
}

#[test]
fn stream_buffer_not_present_pop_err() {
    StreamBuffer::<()>::NotPresent
        .pop_front()
        .expect_err("should be an error");
}

#[test]
fn stream_buffer_dropped_push() {
    // Use an Arc and a Weak copy of it as an indicator of whether the data is still alive
    let data = Arc::new(1);
    let data_weak = Arc::downgrade(&data);
    let mut dropped = StreamBuffer::Dropped { ended: false };
    dropped
        .push_back(Some(data))
        .expect("can't push on dropped");
    // Should still be dropped - i.e., the message is not stored
    assert!(matches!(dropped, StreamBuffer::Dropped { ended: false }));
    // Pushing none should set the ended flag
    dropped.push_back(None).expect("can't push on dropped");
    assert!(matches!(dropped, StreamBuffer::Dropped { ended: true }));
    // The data itself should also have been dropped - i.e., there are no copies of it around
    assert!(data_weak.upgrade().is_none(), "dropped data was preserved");
}

#[test]
fn stream_buffer_dropped_pop_err() {
    StreamBuffer::<()>::Dropped { ended: false }
        .pop_front()
        .expect_err("should be an error");
}
