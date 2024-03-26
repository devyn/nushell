use nu_protocol::{NuString, Value};
use std::collections::HashSet;

pub fn get_columns<'a, T>(input: &'a [Value]) -> Vec<T>
where
    T: From<&'a NuString> + AsRef<str>,
{
    let mut columns = vec![];
    for item in input {
        let Value::Record { val, .. } = item else {
            return vec![];
        };

        for col in val.columns() {
            if !columns.iter().any(|c: &T| col.as_str() == c.as_ref()) {
                columns.push(col.into());
            }
        }
    }

    columns
}

// If a column doesn't exist in the input, return it.
pub fn nonexistent_column<'a>(
    inputs: &[NuString],
    columns: impl IntoIterator<Item = &'a NuString>,
) -> Option<NuString> {
    let set: HashSet<&NuString> = HashSet::from_iter(columns);

    for input in inputs {
        if set.contains(&input) {
            continue;
        }
        return Some(input.clone());
    }
    None
}
