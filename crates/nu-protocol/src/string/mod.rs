use std::{sync::Arc, fmt, ops::Deref, cmp::Ordering, hash::Hash, borrow::Cow};

use serde::{Serialize, Deserialize};

#[cfg(test)]
mod tests;

/// We automatically create shared strings for strings that are this length or below
const SHARED_STRING_MAX_LEN: usize = 255;

/// An efficient implementation of a string for use within the parser and interpreter.
///
/// The exact implementation is subject to change. This provides us a layer of abstraction so we
/// can experiment with the most efficient ways to store different types of strings.
///
/// This is intended to be mostly a drop-in replacement for `String`, but it may be missing some
/// methods. The trait implementations should generally work identically.
#[repr(transparent)]
pub struct NuString(Variant);

enum Variant {
    Empty,
    Owned(String),
    Shared(Arc<str>),
}

impl NuString {
    /// Create a new empty string.
    pub const fn new() -> Self {
        NuString(Variant::Empty)
    }

    /// Get the string as a string slice.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Variant::Empty => "",
            Variant::Owned(string) => string.as_str(),
            Variant::Shared(arc) => arc.deref(),
        }
    }

    /// Mutate the internal string, converting to `Owned` if necessary to do so.
    fn string_mut(&mut self) -> &mut String {
        match self.0 {
            Variant::Owned(ref mut s) => s,
            _ => {
                // Convert the string to Owned, then return that mutable reference
                *self = NuString(Variant::Owned(self.as_str().into()));
                self.string_mut()
            }
        }
    }

    /// Suggest that the string should be cached, even if it makes modifying it more expensive.
    ///
    /// The exact implementation of this is not defined, and it may involve copying the whole
    /// string in some cases.
    pub fn intern(self) -> Self {
        match self.0 {
            Variant::Empty => self,
            Variant::Shared(_) => self,
            Variant::Owned(s) => NuString(Variant::Shared(s.into())),
        }
    }

    /// Append a character onto this string.
    pub fn push(&mut self, ch: char) {
        self.string_mut().push(ch)
    }

    /// Append a string slice onto this string.
    pub fn push_str(&mut self, s: &str) {
        self.string_mut().push_str(s)
    }
}

impl Deref for NuString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Debug for NuString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.deref(), f)
    }
}

impl fmt::Display for NuString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.deref(), f)
    }
}

impl PartialEq for NuString {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Eq for NuString { }

impl PartialOrd for NuString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Ord for NuString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}

impl Hash for NuString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}

impl Default for NuString {
    fn default() -> Self {
        NuString::new()
    }
}

impl Serialize for NuString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        self.deref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NuString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        <&'de str as Deserialize>::deserialize(deserializer).map(NuString::from)
    }
}

impl From<String> for NuString {
    fn from(s: String) -> Self {
        if s.len() > SHARED_STRING_MAX_LEN {
            NuString(Variant::Owned(s))
        } else if s.is_empty() {
            NuString(Variant::Empty)
        } else {
            NuString(Variant::Shared(s.into()))
        }
    }
}

impl<'a> From<&'a str> for NuString {
    fn from(s: &'a str) -> Self {
        if s.len() > SHARED_STRING_MAX_LEN {
            NuString(Variant::Owned(s.into()))
        } else if s.is_empty() {
            NuString(Variant::Empty)
        } else {
            NuString(Variant::Shared(s.into()))
        }
    }
}

impl<'a> From<Cow<'a, str>> for NuString {
    fn from(s: Cow<'a, str>) -> Self {
        match s {
            Cow::Borrowed(s) => s.into(),
            Cow::Owned(s) => s.into(),
        }
    }
}

impl From<NuString> for String {
    fn from(value: NuString) -> Self {
        match value.0 {
            Variant::Empty => String::new(),
            Variant::Owned(s) => s,
            Variant::Shared(s) => s.deref().into(),
        }
    }
}

impl<'a> From<NuString> for Cow<'a, str> {
    fn from(value: NuString) -> Self {
        Cow::Owned(String::from(value))
    }
}

impl<'a> Extend<&'a str> for NuString {
    fn extend<T: IntoIterator<Item = &'a str>>(&mut self, iter: T) {
        self.string_mut().extend(iter)
    }
}

impl Extend<String> for NuString {
    fn extend<T: IntoIterator<Item = String>>(&mut self, iter: T) {
        self.string_mut().extend(iter)
    }
}

impl FromIterator<char> for NuString {
    fn from_iter<T: IntoIterator<Item = char>>(iter: T) -> Self {
        String::from_iter(iter).into()
    }
}

impl<'a> FromIterator<&'a str> for NuString {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        String::from_iter(iter).into()
    }
}

impl<'a> FromIterator<String> for NuString {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        String::from_iter(iter).into()
    }
}

