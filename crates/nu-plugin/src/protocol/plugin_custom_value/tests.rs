use nu_protocol::{ast::RangeInclusion, CustomValue, Range, ShellError, Span, Value, record};
use serde::{Deserialize, Serialize};

use crate::plugin::PluginIdentity;

use super::PluginCustomValue;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestCustomValue(i32);

#[typetag::serde]
impl CustomValue for TestCustomValue {
    fn clone_value(&self, span: Span) -> Value {
        Value::custom_value(Box::new(self.clone()), span)
    }

    fn value_string(&self) -> String {
        "TestCustomValue".into()
    }

    fn to_base_value(&self, span: Span) -> Result<Value, ShellError> {
        Ok(Value::int(self.0 as i64, span))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn test_plugin_custom_value() -> PluginCustomValue {
    PluginCustomValue {
        name: "TestCustomValue".into(),
        data: vec![0xff, 0xff, 0xff, 0xff], // -1
        source: None,
    }
}

fn test_plugin_custom_value_with_source() -> PluginCustomValue {
    PluginCustomValue {
        source: Some(PluginIdentity::new_fake("test")),
        ..test_plugin_custom_value()
    }
}

#[test]
fn serialize_deserialize() -> Result<(), ShellError> {
    let original_value = TestCustomValue(32);
    let span = Span::test_data();
    let serialized = PluginCustomValue::serialize_from_custom_value(&original_value, span)?;
    assert_eq!(original_value.value_string(), serialized.name);
    assert!(serialized.source.is_none());
    let deserialized = serialized.deserialize_to_custom_value(span)?;
    let downcasted = deserialized
        .as_any()
        .downcast_ref::<TestCustomValue>()
        .expect("failed to downcast: not TestCustomValue");
    assert_eq!(original_value, *downcasted);
    Ok(())
}

#[test]
fn add_source_at_root() -> Result<(), ShellError> {
    let mut val = Value::test_custom_value(Box::new(test_plugin_custom_value()));
    let source = PluginIdentity::new_fake("foo");
    PluginCustomValue::add_source(&mut val, &source);

    let custom_value = val.as_custom_value()?;
    let plugin_custom_value: &PluginCustomValue = custom_value
        .as_any()
        .downcast_ref()
        .expect("not PluginCustomValue");
    assert_eq!(Some(source), plugin_custom_value.source);
    Ok(())
}

#[test]
fn add_source_nested_range() -> Result<(), ShellError> {
    let orig_custom_val = Value::test_custom_value(Box::new(test_plugin_custom_value()));
    let mut val = Value::test_range(Range {
        from: orig_custom_val.clone(),
        incr: orig_custom_val.clone(),
        to: orig_custom_val.clone(),
        inclusion: RangeInclusion::Inclusive,
    });
    let source = PluginIdentity::new_fake("foo");
    PluginCustomValue::add_source(&mut val, &source);

    let range = val.as_range()?;
    for (name, val) in [
        ("from", &range.from),
        ("incr", &range.incr),
        ("to", &range.to),
    ] {
        let custom_value = val
            .as_custom_value()
            .expect(&format!("{name} not custom value"));
        let plugin_custom_value: &PluginCustomValue = custom_value
            .as_any()
            .downcast_ref()
            .expect(&format!("{name} not PluginCustomValue"));
        assert_eq!(
            Some(&source),
            plugin_custom_value.source.as_ref(),
            "{name} source not set correctly"
        );
    }
    Ok(())
}

#[test]
fn add_source_nested_record() -> Result<(), ShellError> {
    let orig_custom_val = Value::test_custom_value(Box::new(test_plugin_custom_value()));
    let mut val = Value::test_record(record!{
        "foo" => orig_custom_val.clone(),
        "bar" => orig_custom_val.clone(),
    });
    let source = PluginIdentity::new_fake("foo");
    PluginCustomValue::add_source(&mut val, &source);

    let record = val.as_record()?;
    for key in ["foo", "bar"] {
        let val = record.get(key).expect(&format!("record does not contain '{key}'"));
        let custom_value = val
            .as_custom_value()
            .expect(&format!("'{key}' not custom value"));
        let plugin_custom_value: &PluginCustomValue = custom_value
            .as_any()
            .downcast_ref()
            .expect(&format!("'{key}' not PluginCustomValue"));
        assert_eq!(
            Some(&source),
            plugin_custom_value.source.as_ref(),
            "'{key}' source not set correctly"
        );
    }
    Ok(())
}

#[test]
fn add_source_nested_list() -> Result<(), ShellError> {
    let orig_custom_val = Value::test_custom_value(Box::new(test_plugin_custom_value()));
    let mut val = Value::test_list(vec![
        orig_custom_val.clone(),
        orig_custom_val.clone(),
    ]);
    let source = PluginIdentity::new_fake("foo");
    PluginCustomValue::add_source(&mut val, &source);

    let list = val.as_list()?;
    for (index, val) in list.iter().enumerate() {
        let custom_value = val
            .as_custom_value()
            .expect(&format!("[{index}] not custom value"));
        let plugin_custom_value: &PluginCustomValue = custom_value
            .as_any()
            .downcast_ref()
            .expect(&format!("[{index}] not PluginCustomValue"));
        assert_eq!(
            Some(&source),
            plugin_custom_value.source.as_ref(),
            "[{index}] source not set correctly"
        );
    }
    Ok(())
}

#[test]
fn verify_source_error_message() -> Result<(), ShellError> {
    let span = Span::new(5, 7);
    let mut ok_val = Value::custom_value(Box::new(test_plugin_custom_value_with_source()), span);
    let mut native_val = Value::custom_value(Box::new(TestCustomValue(32)), span);
    let mut foreign_val = {
        let mut val = test_plugin_custom_value();
        val.source = Some(PluginIdentity::new_fake("other"));
        Value::custom_value(Box::new(val), span)
    };
    let source = PluginIdentity::new_fake("test");

    PluginCustomValue::verify_source(&mut ok_val, &source).expect("ok_val should be verified ok");

    for (val, src_plugin) in [
        (&mut native_val, None),
        (&mut foreign_val, Some("other"))
    ] {
        let error = PluginCustomValue::verify_source(val, &source)
            .expect_err(&format!("a custom value from {src_plugin:?} should result in an error"));
        if let ShellError::CustomValueIncorrectForPlugin { name, span: err_span, dest_plugin, src_plugin: err_src_plugin } = error {
            assert_eq!("TestCustomValue", name, "error.name from {src_plugin:?}");
            assert_eq!(span, err_span, "error.span from {src_plugin:?}");
            assert_eq!("test", dest_plugin, "error.dest_plugin from {src_plugin:?}");
            assert_eq!(src_plugin, err_src_plugin.as_deref(), "error.src_plugin");
        } else {
            panic!("the error returned should be CustomValueIncorrectForPlugin");
        }
    }

    Ok(())
}

#[test]
fn verify_source_nested_range() -> Result<(), ShellError> {
    let native_val = Value::test_custom_value(Box::new(TestCustomValue(32)));
    let source = PluginIdentity::new_fake("test");
    for (name, mut val) in [
        ("from", Value::test_range(Range {
            from: native_val.clone(),
            incr: Value::test_nothing(),
            to: Value::test_nothing(),
            inclusion: RangeInclusion::RightExclusive,
        })),
        ("incr", Value::test_range(Range {
            from: Value::test_nothing(),
            incr: native_val.clone(),
            to: Value::test_nothing(),
            inclusion: RangeInclusion::RightExclusive,
        })),
        ("to", Value::test_range(Range {
            from: Value::test_nothing(),
            incr: Value::test_nothing(),
            to: native_val.clone(),
            inclusion: RangeInclusion::RightExclusive,
        })),
    ] {
        PluginCustomValue::verify_source(&mut val, &source)
            .expect_err(&format!("error not generated on {name}"));
    }

    let mut ok_range = Value::test_range(Range {
        from: Value::test_nothing(),
        incr: Value::test_nothing(),
        to: Value::test_nothing(),
        inclusion: RangeInclusion::RightExclusive,
    });
    PluginCustomValue::verify_source(&mut ok_range, &source)
        .expect("ok_range should not generate error");

    Ok(())
}

#[test]
fn verify_source_nested_record() -> Result<(), ShellError> {
    let native_val = Value::test_custom_value(Box::new(TestCustomValue(32)));
    let source = PluginIdentity::new_fake("test");
    for (name, mut val) in [
        ("first element foo", Value::test_record(record!{
            "foo" => native_val.clone(),
            "bar" => Value::test_nothing(),
        })),
        ("second element bar", Value::test_record(record!{
            "foo" => Value::test_nothing(),
            "bar" => native_val.clone(),
        })),
    ] {
        PluginCustomValue::verify_source(&mut val, &source)
            .expect_err(&format!("error not generated on {name}"));
    }

    let mut ok_record = Value::test_record(record!{"foo" => Value::test_nothing()});
    PluginCustomValue::verify_source(&mut ok_record, &source)
        .expect("ok_record should not generate error");

    Ok(())
}

#[test]
fn verify_source_nested_list() -> Result<(), ShellError> {
    let native_val = Value::test_custom_value(Box::new(TestCustomValue(32)));
    let source = PluginIdentity::new_fake("test");
    for (name, mut val) in [
        ("first element", Value::test_list(vec![native_val.clone(), Value::test_nothing()])),
        ("second element", Value::test_list(vec![Value::test_nothing(), native_val.clone()])),
    ] {
        PluginCustomValue::verify_source(&mut val, &source)
            .expect_err(&format!("error not generated on {name}"));
    }

    let mut ok_list = Value::test_list(vec![Value::test_nothing()]);
    PluginCustomValue::verify_source(&mut ok_list, &source)
        .expect("ok_list should not generate error");

    Ok(())
}

#[test]
#[ignore = "TODO"]
fn serialize_in_root() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn serialize_in_range() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn serialize_in_record() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn serialize_in_list() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn deserialize_in_root() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn deserialize_in_range() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn deserialize_in_record() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn deserialize_in_list() -> Result<(), ShellError> {
    todo!()
}
