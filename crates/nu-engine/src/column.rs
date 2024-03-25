use nu_protocol::{NuString, Value};
use std::{borrow::Borrow, collections::HashSet, hash::Hash};

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
pub fn nonexistent_column<I, S>(inputs: &[NuString], columns: I) -> Option<NuString>
where
    I: IntoIterator<Item = S>,
    NuString: Borrow<S>,
    S: Hash + Eq,
{
    let set: HashSet<S> = HashSet::from_iter(columns);

    for input in inputs {
        if set.contains(input.borrow()) {
            continue;
        }
        return Some(input.clone());
    }
    None
}
