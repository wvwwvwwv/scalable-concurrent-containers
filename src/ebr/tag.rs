/// Returns tags in the pointer.
pub(super) fn tags<P>(ptr: *const P) -> (bool, bool) {
    ((ptr as usize & 1) == 1, (ptr as usize & 2) == 2)
}

/// Sets tags.
pub(super) fn update_tags<P>(ptr: *const P, first_tag: bool, second_tag: bool) -> *const P {
    let tags = match (first_tag, second_tag) {
        (false, false) => 0,
        (true, false) => 1,
        (false, true) => 2,
        (true, true) => 3,
    };
    unsafe { std::mem::transmute(((ptr as usize) & (!3)) | tags) }
}

/// Returns the pointer with the tag field erased.
pub(super) fn unset_tags<P>(ptr: *const P) -> *const P {
    unsafe { std::mem::transmute((ptr as usize) & (!3)) }
}
