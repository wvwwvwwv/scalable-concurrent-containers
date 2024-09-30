//! Vendor the [`equivalent`](https://crates.io/crates/equivalent) crate in order to avoid any conflicts.

use std::{borrow::Borrow, cmp::Ordering};

/// Key equivalence trait.
pub trait Equivalent<K: ?Sized> {
    /// Compares `self` to `key` and returns `true` if they are equal.
    fn equivalent(&self, key: &K) -> bool;
}

impl<Q: ?Sized, K: ?Sized> Equivalent<K> for Q
where
    Q: Eq,
    K: Borrow<Q>,
{
    #[inline]
    fn equivalent(&self, key: &K) -> bool {
        PartialEq::eq(self, key.borrow())
    }
}

/// Key ordering trait.
pub trait Comparable<K: ?Sized>: Equivalent<K> {
    /// Compares `self` to `key` and returns their ordering.
    fn compare(&self, key: &K) -> Ordering;
}

impl<Q: ?Sized, K: ?Sized> Comparable<K> for Q
where
    Q: Ord,
    K: Borrow<Q>,
{
    #[inline]
    fn compare(&self, key: &K) -> Ordering {
        Ord::cmp(self, key.borrow())
    }
}
