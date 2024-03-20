use indexmap::{indexset, IndexSet};
use nu_protocol::{Value, NuString};

pub fn merge_descriptors(values: &[Value]) -> Vec<NuString> {
    let mut ret: Vec<NuString> = vec![];
    let mut seen: IndexSet<NuString> = indexset! {};
    for value in values {
        let data_descriptors = match value {
            Value::Record { val, .. } => val.columns().cloned().collect(),
            _ => vec!["".into()],
        };
        for desc in data_descriptors {
            if !desc.is_empty() && !seen.contains(desc.as_str()) {
                seen.insert(desc.clone());
                ret.push(desc);
            }
        }
    }
    ret
}
