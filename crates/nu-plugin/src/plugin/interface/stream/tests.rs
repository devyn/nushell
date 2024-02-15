use std::{sync::{mpsc, Arc, atomic::{AtomicBool, Ordering::Relaxed}}, time::Duration};

use nu_protocol::{ShellError, Value};

use crate::protocol::{StreamMessage, StreamData};

use super::{StreamReader, WriteStreamMessage, StreamWriterSignal, StreamWriter, StreamManager};

// Should be long enough to definitely complete any quick operation, but not so long that tests are
// slow to complete. 10 ms is a pretty long time
const WAIT_DURATION: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Default)]
struct TestSink(Vec<StreamMessage>);

impl WriteStreamMessage for TestSink {
    fn write_stream_message(&mut self, msg: StreamMessage) -> Result<(), ShellError> {
        self.0.push(msg);
        Ok(())
    }
}

impl WriteStreamMessage for mpsc::Sender<StreamMessage> {
    fn write_stream_message(&mut self, msg: StreamMessage) -> Result<(), ShellError> {
        self.send(msg).map_err(|err| ShellError::NushellFailed {
            msg: err.to_string()
        })
    }
}

#[test]
fn reader_recv_list_messages() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::List(Value::test_int(5))))).unwrap();
    drop(tx);

    assert_eq!(Some(Value::test_int(5)), reader.recv()?);
    Ok(())
}

#[test]
fn list_reader_recv_wrong_type() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::<Value, _>::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::Raw(Ok(vec![10, 20]))))).unwrap();
    tx.send(Ok(Some(StreamData::List(Value::test_nothing())))).unwrap();
    drop(tx);

    reader.recv().expect_err("should be an error");
    reader.recv().expect("should be able to recover");

    Ok(())
}

#[test]
fn reader_recv_raw_messages() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::Raw(Ok(vec![10, 20]))))).unwrap();
    drop(tx);

    assert_eq!(Some(vec![10, 20]), reader.recv()?.transpose()?);
    Ok(())
}

#[test]
fn raw_reader_recv_wrong_type() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::<Result<Vec<u8>, ShellError>, _>::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::List(Value::test_nothing())))).unwrap();
    tx.send(Ok(Some(StreamData::Raw(Ok(vec![10, 20]))))).unwrap();
    drop(tx);

    reader.recv().expect_err("should be an error");
    reader.recv().expect("should be able to recover");

    Ok(())
}

#[test]
fn reader_recv_acknowledge() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::<Value, _>::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::List(Value::test_int(5))))).unwrap();
    tx.send(Ok(Some(StreamData::List(Value::test_int(6))))).unwrap();
    drop(tx);

    reader.recv()?;
    reader.recv()?;
    let wrote = &reader.writer.0;
    assert!(wrote.len() >= 2);
    assert!(matches!(wrote[0], StreamMessage::Ack(0)), "0 = {:?}", wrote[0]);
    assert!(matches!(wrote[1], StreamMessage::Ack(0)), "1 = {:?}", wrote[1]);
    Ok(())
}

#[test]
fn reader_recv_end_of_stream() -> Result<(), ShellError> {
    let (tx, rx) = mpsc::channel();
    let mut reader = StreamReader::<Value, _>::new(0, rx, TestSink::default());

    tx.send(Ok(Some(StreamData::List(Value::test_int(5))))).unwrap();
    tx.send(Ok(None)).unwrap();
    drop(tx);

    assert!(reader.recv()?.is_some(), "actual message");
    assert!(reader.recv()?.is_none(), "on close");
    assert!(reader.recv()?.is_none(), "after close");
    Ok(())
}

#[test]
fn reader_drop() {
    let (_tx, rx) = mpsc::channel();

    // Flag set if drop message is received.
    struct Check(Arc<AtomicBool>);

    impl WriteStreamMessage for Check {
        fn write_stream_message(&mut self, msg: StreamMessage) -> Result<(), ShellError> {
            assert!(matches!(msg, StreamMessage::Drop(1)),  "got {:?}", msg);
            self.0.store(true, Relaxed);
            Ok(())
        }
    }

    let flag = Arc::new(AtomicBool::new(false));

    let reader = StreamReader::<Value, _>::new(1, rx, Check(flag.clone()));
    drop(reader);

    assert!(flag.load(Relaxed));
}

#[test]
fn writer_write_all_stops_if_dropped() -> Result<(), ShellError> {
    let signal = Arc::new(StreamWriterSignal::new(20));
    let id = 1337;
    let mut writer = StreamWriter::new(id, signal.clone(), TestSink::default());

    // Simulate this by having it consume a stream that will actually do the drop halfway through
    let iter = (0..5).map(Value::test_int)
        .chain({
            let mut n = 5;
            std::iter::from_fn(move || {
                // produces numbers 5..10, but drops for the first one
                if n == 5 {
                    signal.set_dropped().unwrap();
                }
                if n < 10 {
                    let value = Value::test_int(n);
                    n += 1;
                    Some(value)
                } else {
                    None
                }
            })
        });

    writer.write_all(iter)?;

    assert!(writer.is_dropped()?);

    let wrote = &writer.writer.0;
    assert_eq!(5, wrote.len(), "length wrong: {wrote:?}");

    for (n, message) in (0..5).zip(wrote) {
        match message {
            StreamMessage::Data(msg_id, StreamData::List(value)) => {
                assert_eq!(id, *msg_id, "id");
                assert_eq!(Value::test_int(n), *value, "value");
            }
            other => panic!("unexpected message: {other:?}")
        }
    }

    Ok(())
}

#[test]
fn writer_end() -> Result<(), ShellError> {
    let signal = Arc::new(StreamWriterSignal::new(20));
    let mut writer = StreamWriter::new(9001, signal.clone(), TestSink::default());

    writer.end()?;
    writer.write(Value::test_int(2)).expect_err("shouldn't be able to write after end");
    writer.end().expect("end twice should be ok");

    let wrote = &writer.writer.0;
    assert!(
        matches!(wrote.last(), Some(StreamMessage::End(9001))),
        "didn't write end message: {wrote:?}"
    );

    Ok(())
}

#[test]
fn signal_set_dropped() -> Result<(), ShellError> {
    let signal = StreamWriterSignal::new(4);
    assert!(!signal.is_dropped()?);
    signal.set_dropped()?;
    assert!(signal.is_dropped()?);
    Ok(())
}

#[test]
fn signal_notify_sent_wont_block_if_flowing() -> Result<(), ShellError> {
    let signal = StreamWriterSignal::new(1);
    std::thread::scope(|scope| {
        let spawned = scope.spawn(|| {
            for _ in 0..100 {
                signal.notify_sent()?;
            }
            Ok(())
        });
        for _ in 0..100 {
            signal.notify_acknowledged()?;
        }
        std::thread::sleep(WAIT_DURATION);
        assert!(spawned.is_finished(), "blocked");
        spawned.join().unwrap()
    })
}

#[test]
fn signal_notify_blocks_on_unacknowledged() -> Result<(), ShellError> {
    let signal = StreamWriterSignal::new(50);
    std::thread::scope(|scope| {
        let spawned = scope.spawn(|| {
            for _ in 0..100 {
                signal.notify_sent()?;
            }
            Ok(())
        });
        std::thread::sleep(WAIT_DURATION);
        assert!(!spawned.is_finished(), "didn't block");
        for _ in 0..100 {
            signal.notify_acknowledged()?;
        }
        std::thread::sleep(WAIT_DURATION);
        assert!(spawned.is_finished(), "blocked at end");
        spawned.join().unwrap()
    })
}

#[test]
fn signal_notify_unblocks_on_dropped() -> Result<(), ShellError> {
    let signal = StreamWriterSignal::new(1);
    std::thread::scope(|scope| {
        let spawned = scope.spawn(|| {
            while !signal.is_dropped()? {
                signal.notify_sent()?;
            }
            Ok(())
        });
        std::thread::sleep(WAIT_DURATION);
        assert!(!spawned.is_finished(), "didn't block");
        signal.set_dropped()?;
        std::thread::sleep(WAIT_DURATION);
        assert!(spawned.is_finished(), "still blocked at end");
        spawned.join().unwrap()
    })
}

#[test]
fn stream_manager_read_scenario() -> Result<(), ShellError> {
    let manager = StreamManager::new();
    let handle = manager.get_handle();
    let (tx, rx) = mpsc::channel();
    let readable = handle.read_stream::<Value, _>(2, tx)?;

    let expected_values = vec![
        Value::test_int(40),
        Value::test_string("hello"),
    ];

    for value in &expected_values {
        manager.handle_message(StreamMessage::Data(2, value.clone().into()))?;
    }
    manager.handle_message(StreamMessage::End(2))?;

    let values = readable.collect::<Vec<Value>>();

    assert_eq!(expected_values, values);

    // Now check the sent messages on consumption
    // Should be Ack for each message, then Drop
    for _ in &expected_values {
        match rx.try_recv().expect("failed to receive Ack") {
            StreamMessage::Ack(2) => (),
            other => panic!("should have been an Ack: {other:?}"),
        }
    }
    match rx.try_recv().expect("failed to receive Drop") {
        StreamMessage::Drop(2) => (),
        other => panic!("should have been a Drop: {other:?}"),
    }

    Ok(())
}

#[test]
fn stream_manager_write_scenario() -> Result<(), ShellError> {
    let manager = StreamManager::new();
    let handle = manager.get_handle();
    let (tx, rx) = mpsc::channel();
    let mut writable = handle.write_stream(4, tx, 100)?;

    let expected_values = vec![
        b"hello".to_vec(),
        b"world".to_vec(),
        b"test".to_vec(),
    ];

    for value in &expected_values {
        writable.write(Ok(value.clone()))?;
    }

    // Now try signalling ack
    assert_eq!(
        expected_values.len() as i32,
        writable.signal.lock()?.unacknowledged,
        "unacknowledged initial count",
    );
    manager.handle_message(StreamMessage::Ack(4))?;
    assert_eq!(
        expected_values.len() as i32 - 1,
        writable.signal.lock()?.unacknowledged,
        "unacknowledged post-Ack count",
    );

    // ...and Drop
    manager.handle_message(StreamMessage::Drop(4))?;
    assert!(writable.is_dropped()?);

    // Drop the StreamWriter...
    drop(writable);

    // now check what was actually written
    for value in &expected_values {
        match rx.try_recv().expect("failed to receive Data") {
            StreamMessage::Data(4, StreamData::Raw(Ok(received))) => {
                assert_eq!(*value, received);
            }
            other @ StreamMessage::Data(..) => panic!("wrong Data for {value:?}: {other:?}"),
            other => panic!("should have been Data: {other:?}"),
        }
    }
    match rx.try_recv().expect("failed to receive End") {
        StreamMessage::End(4) => (),
        other => panic!("should have been End: {other:?}"),
    }

    Ok(())
}

#[test]
fn stream_manager_broadcast_read_error() -> Result<(), ShellError> {
    let manager = StreamManager::new();
    let handle = manager.get_handle();
    let mut readable0 = handle.read_stream::<Value, _>(0, TestSink::default())?;
    let mut readable1 = handle.read_stream::<Result<Vec<u8>, _>, _>(1, TestSink::default())?;

    let error = ShellError::PluginFailedToDecode { msg: "test decode error".into() };

    manager.broadcast_read_error(error.clone())?;
    drop(manager);

    assert_eq!(
        error.to_string(),
        readable0.recv()
            .transpose()
            .expect("nothing received from readable0")
            .expect_err("not an error received from readable0")
            .to_string()
    );
    assert_eq!(
        error.to_string(),
        readable1.next()
            .expect("nothing received from readable1")
            .expect_err("not an error received from readable1")
            .to_string()
    );
    Ok(())
}

#[test]
fn stream_manager_drop_writers_on_drop() -> Result<(), ShellError> {
    let manager = StreamManager::new();
    let handle = manager.get_handle();
    let writable = handle.write_stream(4, TestSink::default(), 100)?;

    assert!(!writable.is_dropped()?);

    drop(manager);

    assert!(writable.is_dropped()?);

    Ok(())
}
