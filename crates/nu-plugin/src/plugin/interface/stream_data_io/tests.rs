use crate::{plugin::interface::buffers::TypedStreamBuffer, protocol::StreamId};

use super::{StreamDataIoBase, StreamDataRead};

/// Extension trait for additional test methods that can be used on any [StreamDataIoBase]
pub(crate) trait StreamDataIoTestExt: StreamDataIoBase {
    /// Define a list stream
    fn def_list(&self, id: StreamId) {
        self.lock_read()
            .stream_buffers()
            .insert(id, TypedStreamBuffer::new_list())
            .unwrap();
    }
    /// Define a raw stream
    fn def_raw(&self, id: StreamId) {
        self.lock_read()
            .stream_buffers()
            .insert(id, TypedStreamBuffer::new_raw())
            .unwrap();
    }
    /// Define external streams
    fn def_external(&self, out: StreamId, err: StreamId, exit_code: StreamId) {
        self.def_raw(out);
        self.def_raw(err);
        self.def_list(exit_code);
    }
}

impl<T: StreamDataIoBase> StreamDataIoTestExt for T {}

macro_rules! gen_stream_data_tests {
    (
        $read_type:ident ($add_read:ident),
        $write_type:ident ($get_write:ident),
        |$test:ident| $gen_interface_impl:expr
    ) => {
        #[test]
        fn read_list_matches_input() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::List(Some(Value::test_bool(true))),
            ));
            $test.$add_read($read_type::StreamData(0, StreamData::List(None)));

            let interface = $gen_interface_impl;
            interface.def_list(0);

            match interface.clone().read_list(0).unwrap() {
                Some(value) => assert_eq!(value, Value::test_bool(true)),
                None => panic!("expected to read list value, got end of list"),
            }

            match interface.clone().read_list(0).unwrap() {
                Some(value) => panic!("expected to read end of list, got {value:?}"),
                None => (),
            }

            interface
                .read_list(0)
                .expect_err("didn't err on end of input");
        }

        #[test]
        fn read_list_multi_matches_input() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                9,
                StreamData::List(Some(Value::test_bool(true))),
            ));
            $test.$add_read($read_type::StreamData(9, StreamData::List(None)));
            $test.$add_read($read_type::StreamData(
                7,
                StreamData::List(Some(Value::test_int(10))),
            ));
            $test.$add_read($read_type::StreamData(7, StreamData::List(None)));

            let interface = $gen_interface_impl;
            interface.def_list(7);
            interface.def_list(9);

            match interface.clone().read_list(9).unwrap() {
                Some(value) => assert_eq!(value, Value::test_bool(true)),
                None => panic!("expected to read list value, got end of list"),
            }

            match interface.clone().read_list(9).unwrap() {
                Some(value) => panic!("expected to read end of list, got {value:?}"),
                None => (),
            }

            match interface.clone().read_list(7).unwrap() {
                Some(value) => assert_eq!(value, Value::test_int(10)),
                None => panic!("expected to read list value, got end of list"),
            }

            match interface.clone().read_list(7).unwrap() {
                Some(value) => panic!("expected to read end of list, got {value:?}"),
                None => (),
            }

            interface
                .read_list(7)
                .expect_err("didn't err on end of input");
        }

        #[test]
        fn read_list_multi_out_of_order_matches_input() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                82,
                StreamData::List(Some(Value::test_int(82))),
            ));
            $test.$add_read($read_type::StreamData(
                48,
                StreamData::List(Some(Value::test_int(48))),
            ));
            $test.$add_read($read_type::StreamData(
                82,
                StreamData::List(Some(Value::test_int(89))),
            ));
            $test.$add_read($read_type::StreamData(
                48,
                StreamData::List(Some(Value::test_int(10))),
            ));
            $test.$add_read($read_type::StreamData(48, StreamData::List(None)));
            $test.$add_read($read_type::StreamData(82, StreamData::List(None)));

            let interface = $gen_interface_impl;
            interface.def_list(48);
            interface.def_list(82);

            let mut stream_82 = vec![];
            while let Some(value) = interface.clone().read_list(82).unwrap() {
                stream_82.push(value);
            }

            let mut stream_48 = vec![];
            while let Some(value) = interface.clone().read_list(48).unwrap() {
                stream_48.push(value);
            }

            assert_eq!(vec![Value::test_int(48), Value::test_int(10)], stream_48);
            assert_eq!(vec![Value::test_int(82), Value::test_int(89)], stream_82);

            interface
                .clone()
                .read_list(48)
                .expect_err("didn't err on end of input 48");
            interface
                .clone()
                .read_list(82)
                .expect_err("didn't err on end of input 82");
        }

        #[test]
        fn read_external_matches_input() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![67]))),
            ));
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![68]))),
            ));
            $test.$add_read($read_type::StreamData(
                2,
                StreamData::List(Some(Value::test_int(1))),
            ));
            $test.$add_read($read_type::StreamData(2, StreamData::List(None)));

            let interface = $gen_interface_impl;
            interface.def_external(0, 1, 2);

            match interface.clone().read_raw(0).expect("while reading stdout") {
                Some(buffer) => assert_eq!(buffer, vec![67]),
                None => panic!("unexpected end of stdout stream"),
            }

            match interface.clone().read_raw(1).expect("while reading stderr") {
                Some(buffer) => assert_eq!(buffer, vec![68]),
                None => panic!("unexpected end of stderr stream"),
            }

            match interface
                .clone()
                .read_list(2)
                .expect("while reading exit code")
            {
                Some(value) => assert_eq!(value, Value::test_int(1)),
                None => panic!("unexpected end of exit code stream"),
            }

            match interface
                .clone()
                .read_list(2)
                .expect("while reading exit code")
            {
                Some(value) => {
                    panic!("unexpected value in exit code stream, expected end: {value:?}")
                }
                None => (),
            }
        }

        #[test]
        fn read_external_streams_out_of_input_order() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![43]))),
            ));
            $test.$add_read($read_type::StreamData(
                2,
                StreamData::List(Some(Value::test_int(42))),
            ));
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![70]))),
            ));

            let interface = $gen_interface_impl;
            interface.def_external(0, 1, 2);

            match interface.clone().read_raw(0).expect("while reading stdout") {
                Some(buffer) => assert_eq!(buffer, vec![70]),
                None => panic!("unexpected end of stdout stream"),
            }

            match interface.clone().read_raw(1).expect("while reading stderr") {
                Some(buffer) => assert_eq!(buffer, vec![43]),
                None => panic!("unexpected end of stderr stream"),
            }

            match interface.read_list(2).expect("while reading exit code") {
                Some(value) => assert_eq!(value, Value::test_int(42)),
                None => panic!("unexpected end of exit code stream"),
            }
        }

        #[test]
        fn read_external_streams_skip_dropped_raw() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![1]))),
            ));
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![2]))),
            ));
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![3]))),
            ));
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![42]))),
            ));
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![4]))),
            ));
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![43]))),
            ));

            let interface = $gen_interface_impl;
            interface.def_external(0, 1, 2);

            interface.drop_raw(0);
            interface
                .clone()
                .read_raw(0)
                .expect_err("reading from dropped stream should be err");
            assert_eq!(interface.clone().read_raw(1).unwrap(), Some(vec![42]));
            assert_eq!(interface.clone().read_raw(1).unwrap(), Some(vec![43]));
            {
                let mut read = interface.read.lock().unwrap();
                let buf = read.stream_buffers.get(0).unwrap();

                if let TypedStreamBuffer::Raw(buf) = buf {
                    assert!(buf.is_dropped());
                } else {
                    panic!("not raw");
                }
            }
            interface
                .read_raw(0)
                .expect_err("reading from dropped stream should be err");
        }

        #[test]
        fn read_external_streams_skip_dropped_list() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![2]))),
            ));
            $test.$add_read($read_type::StreamData(
                2,
                StreamData::List(Some(Value::test_int(1))),
            ));
            $test.$add_read($read_type::StreamData(
                1,
                StreamData::Raw(Some(Ok(vec![3]))),
            ));
            $test.$add_read($read_type::StreamData(
                0,
                StreamData::Raw(Some(Ok(vec![42]))),
            ));

            let interface = $gen_interface_impl;
            interface.def_external(0, 1, 2);

            interface.drop_list(2);
            interface
                .clone()
                .read_list(2)
                .expect_err("reading from dropped stream should be err");
            assert_eq!(interface.clone().read_raw(1).unwrap(), Some(vec![2]));
            assert_eq!(interface.clone().read_raw(1).unwrap(), Some(vec![3]));
            assert_eq!(interface.clone().read_raw(0).unwrap(), Some(vec![42]));
            {
                let mut read = interface.read.lock().unwrap();
                let buf = read.stream_buffers.get(2).unwrap();

                if let TypedStreamBuffer::List(buf) = buf {
                    assert!(buf.is_dropped());
                } else {
                    panic!("not list");
                }
            }
            interface
                .read_list(2)
                .expect_err("reading from dropped stream should be err");
        }

        #[test]
        fn read_error_passthrough() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            let test_msg = "test io error";
            $test.set_read_error(ShellError::IOError {
                msg: test_msg.into(),
            });

            let interface = $gen_interface_impl;
            interface.def_list(0);

            match interface.read_list(0).expect_err("succeeded unexpectedly") {
                ShellError::IOError { msg } => assert_eq!(test_msg, msg),
                other => panic!("other error: {other}"),
            }
        }

        #[test]
        fn write_error_passthrough() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            let test_msg = "test io error";
            $test.set_write_error(ShellError::IOError {
                msg: test_msg.into(),
            });

            let interface = $gen_interface_impl;

            match interface
                .write_list(0, None)
                .expect_err("succeeded unexpectedly")
            {
                ShellError::IOError { msg } => assert_eq!(test_msg, msg),
                other => panic!("other error: {other}"),
            }
            assert!(!$test.has_unconsumed_write());
        }

        #[test]
        fn write_list() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            let data = [Some(Value::test_int(1)), Some(Value::test_int(2)), None];
            let interface = $gen_interface_impl;
            for item in data.iter() {
                interface.write_list(0, item.clone()).expect("write failed");
            }
            for item in data.iter() {
                match $test.$get_write() {
                    Some($write_type::StreamData(0, StreamData::List(read_item))) => {
                        assert_eq!(item, &read_item)
                    }
                    Some(other) => panic!("got other data: {other:?}"),
                    None => panic!("no data was written for {item:?}"),
                }
            }
            assert!(!$test.has_unconsumed_write());
        }

        #[test]
        fn write_raw() {
            use $crate::plugin::interface::stream_data_io::*;

            let $test = TestCase::new();
            let data = [
                Some(Ok(vec![42])),
                Some(Ok(vec![80, 40])),
                Some(Err(ShellError::IOError {
                    msg: "test io error".into(),
                })),
                None,
            ];
            let interface = $gen_interface_impl;
            for item in data.iter() {
                interface.write_raw(0, item.clone()).expect("write failed");
            }
            for item in data.iter() {
                match $test.$get_write() {
                    Some($write_type::StreamData(0, StreamData::Raw(read_item))) => {
                        match (item, &read_item) {
                            (Some(Ok(a)), Some(Ok(b))) => assert_eq!(a, b),
                            (Some(Err(a)), Some(Err(b))) => {
                                assert_eq!(a.to_string(), b.to_string())
                            }
                            (None, None) => (),
                            _ => panic!("expected {item:?}, got {read_item:?}"),
                        }
                    }
                    Some(other) => panic!("got other data: {other:?}"),
                    None => panic!("no data was written for {item:?}"),
                }
            }
            assert!(!$test.has_unconsumed_write());
        }
    };
}

pub(crate) use gen_stream_data_tests;
