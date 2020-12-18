use std::borrow::Cow;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Range};
use std::str::FromStr;

use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum ParseError {
    #[error("Invalid Escape: ~{0}")]
    InvalidEscape(char),
    #[error("Missing Leading \"/\"")]
    NoLeadingSlash,
}
#[derive(Clone, Debug, Error)]
pub enum IndexError {
    #[error("Could Not Index Into {0}")]
    CouldNotIndexInto(&'static str),
    #[error("Index Out Of Bounds: {0}")]
    IndexOutOfBounds(usize),
    #[error("InvalidArrayIndex: {0}")]
    InvalidArrayIndex(#[from] std::num::ParseIntError),
}

#[derive(Clone, Debug)]
pub struct JsonPointer<S: AsRef<str>> {
    src: S,
    segments: VecDeque<PtrSegment>,
}
impl<S: AsRef<str>> JsonPointer<S> {
    pub fn parse(s: S) -> Result<Self, ParseError> {
        let src = s.as_ref();
        let mut segments = VecDeque::new();
        let mut segment = PtrSegment::Unescaped(0..0);
        let mut escape_next_char = false;
        for (idx, c) in src.char_indices() {
            if idx == 0 {
                if c == '/' {
                    continue;
                } else {
                    return Err(ParseError::NoLeadingSlash);
                }
            }
            if escape_next_char {
                match c {
                    '0' => {
                        segment = match segment {
                            PtrSegment::Unescaped(range) => PtrSegment::Escaped(
                                range.start..idx + 1,
                                src[range].to_owned() + "~",
                            ),
                            PtrSegment::Escaped(range, s) => {
                                PtrSegment::Escaped(range.start..idx + 1, s + "~")
                            }
                        }
                    }
                    '1' => {
                        segment = match segment {
                            PtrSegment::Unescaped(range) => PtrSegment::Escaped(
                                range.start..idx + 1,
                                src[range].to_owned() + "/",
                            ),
                            PtrSegment::Escaped(range, s) => {
                                PtrSegment::Escaped(range.start..idx + 1, s + "/")
                            }
                        }
                    }
                    _ => return Err(ParseError::InvalidEscape(c)),
                }
                escape_next_char = false;
            } else {
                match c {
                    '/' => {
                        segments.push_back(segment);
                        segment = PtrSegment::Unescaped(idx + 1..idx + 1);
                    }
                    '~' => {
                        escape_next_char = true;
                    }
                    _ => match segment {
                        PtrSegment::Unescaped(ref mut range) => range.end = idx + 1,
                        PtrSegment::Escaped(ref mut range, ref mut s) => {
                            range.end = idx + 1;
                            s.push(c);
                        }
                    },
                }
            }
        }
        Ok(JsonPointer { src: s, segments })
    }
    pub fn get_segment<'a>(&'a self, idx: usize) -> Option<&'a str> {
        match self.segments.get(idx) {
            Some(PtrSegment::Unescaped(range)) => Some(&self.src.as_ref()[range.clone()]),
            Some(PtrSegment::Escaped(_, s)) => Some(&s),
            None => None,
        }
    }
    pub fn len(&self) -> usize {
        self.segments.len()
    }
    pub fn get<'a>(&self, mut doc: &'a Value) -> Option<&'a Value> {
        for seg in self.iter() {
            doc = if doc.is_array() {
                doc.get(seg.parse::<usize>().ok()?)?
            } else {
                doc.get(seg)?
            };
        }
        Some(doc)
    }
    pub fn get_mut<'a>(&self, mut doc: &'a mut Value) -> Option<&'a mut Value> {
        for seg in self.iter() {
            doc = if doc.is_array() {
                doc.get_mut(seg.parse::<usize>().ok()?)?
            } else {
                doc.get_mut(seg)?
            };
        }
        Some(doc)
    }
    pub fn take(&self, mut doc: &mut Value) -> Option<Value> {
        for seg in self.iter() {
            doc = if doc.is_array() {
                doc.get_mut(seg.parse::<usize>().ok()?)?
            } else {
                doc.get_mut(seg)?
            };
        }
        Some(doc.take())
    }
    pub fn set(&self, mut doc: &mut Value, value: Value) -> Result<Option<Value>, IndexError> {
        for (idx, seg) in self.iter().enumerate() {
            doc = match doc {
                Value::Array(ref mut l) => {
                    let num = if seg == "-" { l.len() } else { seg.parse()? };
                    if num == l.len() {
                        if let Some(next) = self.get_segment(idx + 1) {
                            if next == "0" {
                                l.push(Value::Array(Vec::with_capacity(1)));
                            } else {
                                l.push(Value::Object(serde_json::Map::new()))
                            }
                        } else {
                            l.push(value);
                            return Ok(None);
                        }
                    }
                    l.get_mut(num).ok_or(IndexError::IndexOutOfBounds(num))?
                }
                Value::Bool(_) => return Err(IndexError::CouldNotIndexInto("boolean")),
                Value::Null => return Err(IndexError::CouldNotIndexInto("null")),
                Value::Number(_) => return Err(IndexError::CouldNotIndexInto("number")),
                Value::Object(ref mut o) => {
                    if o.get(seg).is_none() {
                        if let Some(next) = self.get_segment(idx + 1) {
                            if next == "0" {
                                o.insert(seg.to_string(), Value::Array(Vec::with_capacity(1)));
                            } else {
                                o.insert(seg.to_string(), Value::Object(serde_json::Map::new()));
                            }
                        } else {
                            o.insert(seg.to_string(), value);
                            return Ok(None);
                        }
                    }
                    o.get_mut(seg).unwrap()
                }
                Value::String(_) => return Err(IndexError::CouldNotIndexInto("string")),
            }
        }
        Ok(Some(std::mem::replace(doc, value)))
    }
    pub fn insert(&self, mut doc: &mut Value, value: Value) -> Result<Option<Value>, IndexError> {
        for (idx, seg) in self.iter().enumerate() {
            doc = match doc {
                Value::Array(ref mut l) => {
                    let num = if seg == "-" { l.len() } else { seg.parse()? };
                    if let Some(next) = self.get_segment(idx + 1) {
                        if next == "0" {
                            l.insert(num, Value::Array(Vec::with_capacity(1)));
                        } else {
                            l.insert(num, Value::Object(serde_json::Map::new()))
                        }
                    } else {
                        l.insert(num, value);
                        return Ok(None);
                    }
                    l.get_mut(num).ok_or(IndexError::IndexOutOfBounds(num))?
                }
                Value::Bool(_) => return Err(IndexError::CouldNotIndexInto("boolean")),
                Value::Null => return Err(IndexError::CouldNotIndexInto("null")),
                Value::Number(_) => return Err(IndexError::CouldNotIndexInto("number")),
                Value::Object(ref mut o) => {
                    if o.get(seg).is_none() {
                        if let Some(next) = self.get_segment(idx + 1) {
                            if next == "0" {
                                o.insert(seg.to_string(), Value::Array(Vec::with_capacity(1)));
                            } else {
                                o.insert(seg.to_string(), Value::Object(serde_json::Map::new()));
                            }
                        } else {
                            o.insert(seg.to_string(), value);
                            return Ok(None);
                        }
                    }
                    o.get_mut(seg).unwrap()
                }
                Value::String(_) => return Err(IndexError::CouldNotIndexInto("string")),
            }
        }
        Ok(Some(std::mem::replace(doc, value)))
    }
    pub fn remove(&self, mut doc: &mut Value) -> Option<Value> {
        for (idx, seg) in self.iter().enumerate() {
            if self.get_segment(idx + 1).is_none() {
                match doc {
                    Value::Array(ref mut l) => {
                        let num = seg.parse().ok()?;
                        if num < l.len() {
                            return Some(l.remove(num));
                        } else {
                            return None;
                        }
                    }
                    Value::Object(ref mut o) => {
                        return o.remove(seg);
                    }
                    _ => return None,
                }
            } else {
                doc = match doc {
                    Value::Array(ref mut arr) => {
                        if seg == "-" && !arr.is_empty() {
                            let arr_len = arr.len();
                            arr.get_mut(arr_len - 1)?
                        } else {
                            arr.get_mut(seg.parse::<usize>().ok()?)?
                        }
                    }
                    Value::Object(ref mut o) => o.get_mut(seg)?,
                    _ => return None,
                };
            }
        }
        None
    }
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
    pub fn to_owned(self) -> JsonPointer<String> {
        JsonPointer {
            src: self.as_ref().to_owned(),
            segments: self.segments,
        }
    }
    pub fn common_prefix<'a, S0: AsRef<str>>(
        &'a self,
        other: &JsonPointer<S0>,
    ) -> JsonPointer<&'a str> {
        let src = self.src.as_ref();
        let mut common = None;
        for (idx, seg) in self.iter().enumerate() {
            if Some(seg) != other.get_segment(idx) {
                break;
            }
            common = Some(idx);
        }
        let common_idx = if let Some(common) = common {
            self.segments
                .get(common)
                .map(PtrSegment::range)
                .map(|r| r.end)
                .unwrap_or(0)
        } else {
            0
        };
        JsonPointer::parse(&src[..common_idx]).unwrap()
    }
    pub fn starts_with<S0: AsRef<str>>(&self, other: &JsonPointer<S0>) -> bool {
        for (idx, seg) in other.iter().enumerate() {
            if self.get_segment(idx) != Some(seg) {
                return false;
            }
        }
        true
    }
    pub fn iter<'a>(&'a self) -> JsonPointerIter<'a, S> {
        JsonPointerIter {
            ptr: self,
            start: 0,
            end: self.segments.len(),
        }
    }
    pub fn into_iter(self) -> JsonPointerIntoIter<S> {
        JsonPointerIntoIter {
            src: self.src,
            iter: self.segments.into_iter(),
        }
    }
}
impl JsonPointer<String> {
    pub fn push_end(&mut self, segment: &str) {
        if segment.is_empty() {
            return;
        }
        let mut escaped = false;
        self.src.push('/');
        let start = self.src.len();
        for c in segment.chars() {
            match c {
                '~' => {
                    self.src += "~0";
                    escaped = true;
                }
                '/' => {
                    self.src += "~1";
                    escaped = true;
                }
                _ => {
                    self.src.push(c);
                }
            }
        }
        self.segments.push_back(if escaped {
            PtrSegment::Escaped(start..self.src.len(), segment.to_string())
        } else {
            PtrSegment::Unescaped(start..self.src.len())
        })
    }
    pub fn push_end_idx(&mut self, segment: usize) {
        use std::fmt::Write;
        let start = self.src.len() + 1;
        write!(self.src, "/{}", segment).unwrap();
        let end = self.src.len();
        self.segments.push_back(PtrSegment::Unescaped(start..end));
    }
    pub fn push_start(&mut self, segment: &str) {
        if segment.is_empty() {
            return;
        }
        let escaped = segment.chars().filter(|c| *c == '~' || *c == '/').count();
        let prefix_len = segment.len() + escaped + 1;
        let mut src = String::with_capacity(self.src.len() + prefix_len);
        src.push('/');
        for c in segment.chars() {
            match c {
                '~' => {
                    src += "~0";
                }
                '/' => {
                    src += "~1";
                }
                _ => {
                    src.push(c);
                }
            }
        }
        src += self.src.as_str();
        for seg in self.segments.iter_mut() {
            let range = seg.range_mut();
            range.start += prefix_len;
            range.end += prefix_len;
        }
        self.segments.push_front(if escaped > 0 {
            PtrSegment::Escaped(1..prefix_len, segment.to_owned())
        } else {
            PtrSegment::Unescaped(1..prefix_len)
        });
    }
    pub fn push_start_idx(&mut self, segment: usize) {
        let mut src = format!("/{}", segment);
        let end = src.len();
        src += self.src.as_str();
        self.segments.insert(0, PtrSegment::Unescaped(1..end));
    }
    pub fn pop_end(&mut self) {
        if let Some(last) = self.segments.pop_back() {
            self.src.truncate(last.range().start - 1)
        }
    }
    pub fn pop_front(&mut self) {
        if let Some(last) = self.segments.pop_front() {
            let range = last.into_range();
            self.src.replace_range(range.start - 1..range.end, "");
        }
    }
    pub fn truncate(&mut self, new_len: usize) {
        if let Some(seg) = self.segments.get(new_len) {
            self.src.truncate(seg.range().start - 1);
            self.segments.truncate(new_len);
        }
    }
    pub fn join(mut self, segment: &str) -> Self {
        self.push_end(segment);
        self
    }
}
impl FromStr for JsonPointer<String> {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JsonPointer::parse(s.to_owned())
    }
}
impl<S: AsRef<str>> AsRef<str> for JsonPointer<S> {
    fn as_ref(&self) -> &str {
        self.src.as_ref()
    }
}
impl<S: AsRef<str>> PartialEq for JsonPointer<S> {
    fn eq(&self, rhs: &Self) -> bool {
        self.segments.len() == rhs.segments.len() && {
            let mut rhs_iter = rhs.iter();
            self.iter().all(|lhs| Some(lhs) == rhs_iter.next())
        }
    }
}
impl<S: AsRef<str>> Hash for JsonPointer<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for seg in self.iter() {
            seg.hash(state);
        }
    }
}
impl<S: AsRef<str>> std::fmt::Display for JsonPointer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.as_ref(), f)
    }
}
impl<'a, S0, S1> Add<&'a JsonPointer<S1>> for JsonPointer<S0>
where
    S0: AsRef<str> + Add<&'a str>,
    S0::Output: AsRef<str>,
    S1: AsRef<str>,
{
    type Output = JsonPointer<S0::Output>;
    fn add(mut self, rhs: &'a JsonPointer<S1>) -> Self::Output {
        let src_len = self.src.as_ref().len();
        self.segments
            .extend(rhs.segments.iter().map(|seg| match seg {
                PtrSegment::Unescaped(range) => {
                    PtrSegment::Unescaped(range.start + src_len..range.end + src_len)
                }
                PtrSegment::Escaped(range, s) => {
                    PtrSegment::Escaped(range.start + src_len..range.end + src_len, s.clone())
                }
            }));
        JsonPointer {
            src: self.src + rhs.src.as_ref(),
            segments: self.segments,
        }
    }
}
impl<'a, S0, S1> AddAssign<&'a JsonPointer<S1>> for JsonPointer<S0>
where
    S0: AsRef<str> + AddAssign<&'a str>,
    S1: AsRef<str>,
{
    fn add_assign(&mut self, rhs: &'a JsonPointer<S1>) {
        let src_len = self.src.as_ref().len();
        self.segments
            .extend(rhs.segments.iter().map(|seg| match seg {
                PtrSegment::Unescaped(range) => {
                    PtrSegment::Unescaped(range.start + src_len..range.end + src_len)
                }
                PtrSegment::Escaped(range, s) => {
                    PtrSegment::Escaped(range.start + src_len..range.end + src_len, s.clone())
                }
            }));
        self.src += rhs.src.as_ref();
    }
}
impl<'de, S> serde::de::Deserialize<'de> for JsonPointer<S>
where
    S: AsRef<str> + serde::de::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(JsonPointer::parse(S::deserialize(deserializer)?).map_err(serde::de::Error::custom)?)
    }
}
impl<S> serde::ser::Serialize for JsonPointer<S>
where
    S: AsRef<str> + serde::ser::Serialize,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::ser::Serializer,
    {
        self.src.serialize(serializer)
    }
}

#[derive(Clone, Debug)]
enum PtrSegment {
    Unescaped(Range<usize>),
    Escaped(Range<usize>, String),
}
impl PtrSegment {
    fn range(&self) -> &Range<usize> {
        match self {
            PtrSegment::Unescaped(range) => range,
            PtrSegment::Escaped(range, _) => range,
        }
    }
    fn range_mut(&mut self) -> &mut Range<usize> {
        match self {
            PtrSegment::Unescaped(range) => range,
            PtrSegment::Escaped(range, _) => range,
        }
    }
    fn into_range(self) -> Range<usize> {
        match self {
            PtrSegment::Unescaped(range) => range,
            PtrSegment::Escaped(range, _) => range,
        }
    }
}

pub struct JsonPointerIter<'a, S: AsRef<str> + 'a> {
    ptr: &'a JsonPointer<S>,
    start: usize,
    end: usize,
}
impl<'a, S: AsRef<str>> Iterator for JsonPointerIter<'a, S> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let ret = self.ptr.get_segment(self.start);
            ret
        } else {
            None
        }
    }
}
impl<'a, S: AsRef<str>> DoubleEndedIterator for JsonPointerIter<'a, S> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            self.end -= 1;
            self.ptr.get_segment(self.end)
        } else {
            None
        }
    }
}

pub struct JsonPointerIntoIter<S: AsRef<str>> {
    src: S,
    iter: std::collections::vec_deque::IntoIter<PtrSegment>,
}
impl<S: AsRef<str>> JsonPointerIntoIter<S> {
    fn next<'a>(&'a mut self) -> Option<Cow<'a, str>> {
        if let Some(seg) = self.iter.next() {
            Some(match seg {
                PtrSegment::Unescaped(range) => Cow::Borrowed(&self.src.as_ref()[range]),
                PtrSegment::Escaped(_, s) => Cow::Owned(s),
            })
        } else {
            None
        }
    }
    fn next_back<'a>(&'a mut self) -> Option<Cow<'a, str>> {
        if let Some(seg) = self.iter.next_back() {
            Some(match seg {
                PtrSegment::Unescaped(range) => Cow::Borrowed(&self.src.as_ref()[range]),
                PtrSegment::Escaped(_, s) => Cow::Owned(s),
            })
        } else {
            None
        }
    }
}
impl<S: AsRef<str>> Iterator for JsonPointerIntoIter<S> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        self.next().map(|s| s.to_string())
    }
}
impl<S: AsRef<str>> DoubleEndedIterator for JsonPointerIntoIter<S> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back().map(|s| s.to_string())
    }
}
