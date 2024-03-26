use super::{NuString, Variant};

#[test]
fn from_empty_string_is_empty_variant() {
    let s = NuString::from("");
    assert!(matches!(s, NuString(Variant::Empty)));
}

#[test]
fn from_short_string_is_shared_variant() {
    let s = NuString::from("abc");
    assert!(matches!(s, NuString(Variant::Shared(_))));
}

#[test]
fn from_long_string_is_owned_variant() {
    let s = NuString::from(String::from_utf8_lossy(&[0x61; 1024]));
    assert!(matches!(s, NuString(Variant::Owned(_))));
}
