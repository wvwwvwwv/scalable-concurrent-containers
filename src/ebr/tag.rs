use std::cmp::PartialEq;

/// [`Tag`] is a four-state `Enum` that can be embedded in a pointer as the two least
/// significant bits of the pointer value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tag {
    /// None tagged.
    None,
    /// The first bit is tagged.
    First,
    /// The second bit is tagged.
    Second,
    /// Both bits are tagged.
    Both,
}

impl Tag {
    /// Interprets the [`Tag`] as an integer.
    #[inline]
    pub(super) const fn value(self) -> usize {
        match self {
            Self::None => 0,
            Self::First => 1,
            Self::Second => 2,
            Self::Both => 3,
        }
    }

    /// Returns the tag embedded in the pointer.
    #[inline]
    pub(super) fn into_tag<P>(ptr: *const P) -> Self {
        match ((ptr as usize & 1) == 1, (ptr as usize & 2) == 2) {
            (false, false) => Tag::None,
            (true, false) => Tag::First,
            (false, true) => Tag::Second,
            _ => Tag::Both,
        }
    }

    /// Sets a tag, overwriting any existing tag in the pointer.
    #[inline]
    pub(super) fn update_tag<P>(ptr: *const P, tag: Tag) -> *const P {
        (((ptr as usize) & (!3)) | tag.value()) as *const P
    }

    /// Returns the pointer with the tag bits erased.
    #[inline]
    pub(super) fn unset_tag<P>(ptr: *const P) -> *const P {
        ((ptr as usize) & (!3)) as *const P
    }
}

impl TryFrom<u8> for Tag {
    type Error = u8;

    #[inline]
    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(Tag::None),
            1 => Ok(Tag::First),
            2 => Ok(Tag::Second),
            3 => Ok(Tag::Both),
            _ => Err(val),
        }
    }
}

impl From<Tag> for u8 {
    #[inline]
    fn from(t: Tag) -> Self {
        match t {
            Tag::None => 0,
            Tag::First => 1,
            Tag::Second => 2,
            Tag::Both => 3,
        }
    }
}
