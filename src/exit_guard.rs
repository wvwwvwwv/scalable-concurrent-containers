//! This module implements a simplified, yet safe version of
//! [`scopeguard`](https://crates.io/crates/scopeguard).

use std::ops::{Deref, DerefMut};

/// [`ExitGuard`] captures the environment and invokes the defined closure at the end of the scope.
pub(crate) struct ExitGuard<T, F: FnOnce(&mut T)> {
    captured: T,
    drop_callback: Option<F>,
}

impl<T, F: FnOnce(&mut T)> ExitGuard<T, F> {
    /// Creates a new [`ExitGuard`] with the specified variables captured.
    #[inline]
    pub(crate) fn new(captured: T, drop_callback: F) -> Self {
        Self {
            captured,
            drop_callback: Some(drop_callback),
        }
    }
}

impl<T, F: FnOnce(&mut T)> Drop for ExitGuard<T, F> {
    #[inline]
    fn drop(&mut self) {
        if let Some(f) = self.drop_callback.take() {
            f(&mut self.captured);
        }
    }
}

impl<T, F: FnOnce(&mut T)> Deref for ExitGuard<T, F> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.captured
    }
}

impl<T, F: FnOnce(&mut T)> DerefMut for ExitGuard<T, F> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.captured
    }
}
