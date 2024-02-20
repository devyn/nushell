use nu_protocol::{ast::RangeInclusion, CustomValue, Range, ShellError, Span, Value};
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
#[ignore = "TODO"]
fn add_source_nested_record() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn add_source_nested_list() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn verify_source_error_message() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn verify_source_nested_range() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn verify_source_nested_record() -> Result<(), ShellError> {
    todo!()
}

#[test]
#[ignore = "TODO"]
fn verify_source_nested_list() -> Result<(), ShellError> {
    todo!()
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
