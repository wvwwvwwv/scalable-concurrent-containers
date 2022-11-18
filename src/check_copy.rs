use std::marker::PhantomData;

/// A helper trait to determine whether any arbitrary type is [`Copy`] or not.
///
/// This code attributes to [`nvzqz/impls`](https://github.com/nvzqz/impls).
pub(crate) trait NotCopy {
    const VALUE: bool = false;
}

impl<T> NotCopy for T {}

pub(crate) struct IsCopy<T>(PhantomData<T>);

impl<T: Copy> IsCopy<T> {
    #[allow(dead_code)]
    const VALUE: bool = true;
}

#[cfg(test)]
mod test {
    #![allow(clippy::assertions_on_constants)]

    use super::*;

    #[test]
    fn is_copy() {
        assert!(IsCopy::<usize>::VALUE);
        assert!(IsCopy::<(usize, usize)>::VALUE);
        assert!(!IsCopy::<String>::VALUE);
        assert!(!IsCopy::<(usize, String)>::VALUE);
    }
}
