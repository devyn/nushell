use std::{
    borrow::{Borrow, Cow},
    cmp::Ordering,
    convert::Infallible,
    ffi::OsStr,
    fmt,
    hash::Hash,
    ops::{self, Deref},
    path::{Path, PathBuf},
    str::FromStr,
    string::FromUtf8Error,
    sync::Arc,
};

use serde::{Deserialize, Serialize};

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

    /// Create a new empty string from UTF-8 encoded text. Returns `Err` if there was a problem with
    /// decoding.
    pub fn from_utf8(encoded: Vec<u8>) -> Result<NuString, FromUtf8Error> {
        String::from_utf8(encoded).map(NuString::from)
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

    /// Convert the string into a byte vector, copying if necessary.
    pub fn into_bytes(self) -> Vec<u8> {
        String::from(self).into_bytes()
    }

    /// Remove the last character from the string and return it.
    pub fn pop(&mut self) -> Option<char> {
        self.string_mut().pop()
    }

    /// Truncate the string to at most the specified length. The remaining characters will be
    /// discarded.
    pub fn truncate(&mut self, length: usize) {
        if self.len() > length {
            self.string_mut().truncate(length)
        }
    }
}

impl Deref for NuString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Clone for NuString {
    fn clone(&self) -> Self {
        // The clone implementation is a little special. A string that gets cloned once might get
        // cloned again, so we could return a Shared string if it's small enough
        match &self.0 {
            Variant::Empty => NuString(Variant::Empty),
            Variant::Owned(s) => {
                if s.len() <= SHARED_STRING_MAX_LEN {
                    NuString(Variant::Shared(s.as_str().into()))
                } else {
                    NuString(Variant::Owned(s.clone()))
                }
            }
            Variant::Shared(s) => NuString(Variant::Shared(s.clone())),
        }
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

impl PartialEq<str> for NuString {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<&'a str> for NuString {
    fn eq(&self, other: &&'a str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for NuString {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<Cow<'a, str>> for NuString {
    fn eq(&self, other: &Cow<'a, str>) -> bool {
        self.as_str() == &*other
    }
}

impl PartialEq<NuString> for str {
    fn eq(&self, other: &NuString) -> bool {
        other.eq(self)
    }
}

impl<'a> PartialEq<NuString> for &'a str {
    fn eq(&self, other: &NuString) -> bool {
        other.eq(self)
    }
}

impl PartialEq<NuString> for String {
    fn eq(&self, other: &NuString) -> bool {
        other.eq(self)
    }
}

impl<'a> PartialEq<NuString> for Cow<'a, str> {
    fn eq(&self, other: &NuString) -> bool {
        other.eq(self)
    }
}

impl Eq for NuString {}

impl PartialOrd for NuString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl PartialOrd<str> for NuString {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl<'a> PartialOrd<&'a str> for NuString {
    fn partial_cmp(&self, other: &&'a str) -> Option<Ordering> {
        self.as_str().partial_cmp(*other)
    }
}

impl PartialOrd<String> for NuString {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl<'a> PartialOrd<Cow<'a, str>> for NuString {
    fn partial_cmp(&self, other: &Cow<'a, str>) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_ref())
    }
}

impl PartialOrd<NuString> for str {
    fn partial_cmp(&self, other: &NuString) -> Option<Ordering> {
        other.partial_cmp(self)
    }
}

impl<'a> PartialOrd<NuString> for &'a str {
    fn partial_cmp(&self, other: &NuString) -> Option<Ordering> {
        other.partial_cmp(self)
    }
}

impl PartialOrd<NuString> for String {
    fn partial_cmp(&self, other: &NuString) -> Option<Ordering> {
        other.partial_cmp(self)
    }
}

impl<'a> PartialOrd<NuString> for Cow<'a, str> {
    fn partial_cmp(&self, other: &NuString) -> Option<Ordering> {
        other.partial_cmp(self)
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
        S: serde::Serializer,
    {
        self.deref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NuString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
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

impl From<char> for NuString {
    fn from(ch: char) -> Self {
        String::from(ch).into()
    }
}

impl<'a> From<&'a String> for NuString {
    fn from(s: &'a String) -> Self {
        s.as_str().into()
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

impl<'a> From<&'a NuString> for NuString {
    fn from(s: &'a NuString) -> Self {
        s.clone()
    }
}

impl From<NuString> for String {
    fn from(s: NuString) -> Self {
        match s.0 {
            Variant::Empty => String::new(),
            Variant::Owned(s) => s,
            Variant::Shared(s) => s.deref().into(),
        }
    }
}

impl<'a> From<&'a NuString> for String {
    fn from(s: &'a NuString) -> Self {
        s.as_str().into()
    }
}

impl<'a> From<NuString> for Cow<'a, str> {
    fn from(s: NuString) -> Self {
        Cow::Owned(String::from(s))
    }
}

impl<'a> From<&'a NuString> for Cow<'a, str> {
    fn from(s: &'a NuString) -> Self {
        Cow::Borrowed(s.as_str())
    }
}

impl<'a> From<&'a NuString> for &'a str {
    fn from(value: &'a NuString) -> Self {
        value.as_str()
    }
}

impl From<NuString> for PathBuf {
    fn from(s: NuString) -> Self {
        String::from(s).into()
    }
}

impl From<NuString> for Vec<u8> {
    fn from(value: NuString) -> Self {
        match value.0 {
            Variant::Empty => vec![],
            Variant::Owned(s) => s.into(),
            Variant::Shared(s) => s.as_bytes().into(),
        }
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

impl Extend<char> for NuString {
    fn extend<T: IntoIterator<Item = char>>(&mut self, iter: T) {
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

impl AsRef<str> for NuString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<OsStr> for NuString {
    fn as_ref(&self) -> &OsStr {
        self.as_str().as_ref()
    }
}

impl AsRef<Path> for NuString {
    fn as_ref(&self) -> &Path {
        self.as_str().as_ref()
    }
}

impl AsRef<[u8]> for NuString {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_ref()
    }
}

impl Borrow<str> for NuString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> Borrow<str> for &'a NuString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Write for NuString {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s);
        Ok(())
    }
}

impl ops::Add for NuString {
    type Output = NuString;

    fn add(self, rhs: Self) -> Self::Output {
        (String::from(self) + rhs.as_str()).into()
    }
}

impl ops::Add<String> for NuString {
    type Output = NuString;

    fn add(self, rhs: String) -> Self::Output {
        (String::from(self) + rhs.as_str()).into()
    }
}

impl<'a> ops::Add<&'a str> for NuString {
    type Output = NuString;

    fn add(self, rhs: &'a str) -> Self::Output {
        (String::from(self) + rhs).into()
    }
}

impl ops::AddAssign for NuString {
    fn add_assign(&mut self, rhs: Self) {
        self.push_str(&rhs);
    }
}

impl ops::AddAssign<String> for NuString {
    fn add_assign(&mut self, rhs: String) {
        self.push_str(&rhs);
    }
}

impl<'a> ops::AddAssign<&'a str> for NuString {
    fn add_assign(&mut self, rhs: &'a str) {
        self.push_str(rhs);
    }
}

impl FromStr for NuString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

pub trait ToNuString: ToString {
    /// Format `self` as a [`NuString`]. This is typically used with types implementing
    /// [`Display`](std::fmt::Display).
    ///
    /// This is just a shorthand wrapper around `.to_string().into()`.
    fn to_nu_string(&self) -> NuString {
        self.to_string().into()
    }
}

impl<T: ToString> ToNuString for T {}
