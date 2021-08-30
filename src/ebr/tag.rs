use core::mem::discriminant;
use std::cmp::PartialEq;

/// [`Tag`] is a four-state enumerator that can be embedded in a pointer as the two least
/// significant bits of the pointer value.
#[derive(Clone, Copy, Debug)]
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
    /// Interpret the [`Tag`] as an integer.
    pub(super) fn value(&self) -> usize {
        match self {
            Self::None => 0,
            Self::First => 1,
            Self::Second => 2,
            Self::Both => 3,
        }
    }

    /// Returns the tag embedded in the pointer.
    pub(super) fn into_tag<P>(ptr: *const P) -> Tag {
        match ((ptr as usize & 1) == 1, (ptr as usize & 2) == 2) {
            (false, false) => Tag::None,
            (true, false) => Tag::First,
            (false, true) => Tag::Second,
            _ => Tag::Both,
        }
    }

    /// Sets a tag, overwriting any existing tag in the pointer.
    pub(super) fn update_tag<P>(ptr: *const P, tag: Tag) -> *const P {
        unsafe { std::mem::transmute(((ptr as usize) & (!3)) | tag.value()) }
    }

    /// Returns the pointer with the tag bits erased.
    pub(super) fn unset_tag<P>(ptr: *const P) -> *const P {
        unsafe { std::mem::transmute((ptr as usize) & (!3)) }
    }
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        discriminant(self) == discriminant(other)
    }
}
