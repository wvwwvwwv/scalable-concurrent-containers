use crate::ebr::{AtomicArc, Barrier, Ptr, Tag};

use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// [`LinkedList`] is a self-referential doubly linked list.
///
/// The default implementation of `push_back` and `pop_self` functions rely on traditional
/// locking mechanisms.
pub trait LinkedList: 'static + Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if the caller of `push_back` or `remove` passes a tag.
    /// The tag is reset as soon as the pointer is replaced with a new one.
    fn forward_link(&self) -> &AtomicArc<Self>;

    /// Returns a reference to the backward link.
    ///
    /// The pointer value may be tagged 1 when the entry is being modified.
    fn backward_link(&self) -> &AtomicArc<Self>;

    /// Adds the given entries after self.
    ///
    /// The forward link pointer is tagged with the given tag to indicate a special state.
    fn push_back<'b>(
        &self,
        self_ptr: Ptr<'b, Self>,
        entries: &[Ptr<'b, Self>],
        tag: Tag,
        barrier: &'b Barrier,
    ) -> bool {
        if entries.is_empty() {
            return false;
        }

        // Locks the next and self.
        let mut lockers = LinkedListLocker::lock_next_self(self, barrier);

        // Connects the given entries one another.
        let mut first_entry_ptr: Ptr<'b, Self> = Ptr::null();
        let mut last_entry_ptr: Ptr<'b, Self> = Ptr::null();
        for entry_ptr in entries.iter() {
            if let Some(entry_ref) = entry_ptr.as_ref() {
                if first_entry_ptr.is_null() {
                    first_entry_ptr = *entry_ptr;
                }
                if let Some(prev_entry) = last_entry_ptr.as_ref() {
                    prev_entry
                        .forward_link()
                        .swap((entry_ptr.try_into_arc(), Tag::None), Relaxed);
                    entry_ref
                        .backward_link()
                        .swap((last_entry_ptr.try_into_arc(), Tag::None), Relaxed);
                }
                last_entry_ptr = *entry_ptr;
            }
        }

        if let Some(first_entry_ref) = first_entry_ptr.as_ref() {
            // Connects the given entries with the next and `self`.
            if let Some(last_entry_ref) = last_entry_ptr.as_ref() {
                last_entry_ref.forward_link().swap(
                    (
                        self.forward_link().load(Relaxed, barrier).try_into_arc(),
                        Tag::None,
                    ),
                    Relaxed,
                );
            }
            first_entry_ref
                .backward_link()
                .swap((self_ptr.try_into_arc(), Tag::None), Relaxed);

            // Makes the next entry point to the last entry.
            if let Some(next_locker) = lockers.0.as_mut() {
                next_locker
                    .entry
                    .backward_link()
                    .swap((last_entry_ptr.try_into_arc(), Tag::First), Relaxed);
            }

            // Makes everything visible.
            self.forward_link()
                .swap((first_entry_ptr.try_into_arc(), tag), Release);

            return true;
        }
        false
    }

    /// Removes `self` from the linked list.
    fn pop_self(&self, tag: Tag, barrier: &Barrier) {
        // Locks the next entry and self.
        let mut lockers = LinkedListLocker::lock_next_self(self, barrier);

        // Locks the previous entry and modifies the linked list.
        let mut prev_entry_ptr = self.backward_link().load(Relaxed, barrier);
        prev_entry_ptr.set_tag(Tag::None);
        if let Some(prev_entry_ref) = prev_entry_ptr.as_ref() {
            // Makes the prev entry point to the next entry.
            let prev_entry_locker = LinkedListLocker::lock(prev_entry_ref, barrier);
            if let Some(next_entry_locker) = lockers.0.as_mut() {
                next_entry_locker
                    .entry
                    .backward_link()
                    .swap((prev_entry_ptr.try_into_arc(), Tag::First), Relaxed);
            }
            prev_entry_locker.entry.forward_link().swap(
                (
                    self.forward_link().load(Relaxed, barrier).try_into_arc(),
                    tag,
                ),
                Release,
            );
            self.backward_link().swap((None, Tag::First), Relaxed);
        } else if let Some(next_entry_locker) = lockers.0.as_mut() {
            next_entry_locker
                .entry
                .backward_link()
                .swap((None, Tag::First), Relaxed);
        }
    }
}

/// Linked list locker.
struct LinkedListLocker<'b, T: LinkedList> {
    entry: &'b T,
}

impl<'b, T: LinkedList> LinkedListLocker<'b, T> {
    /// Locks a single entry.
    fn lock(entry: &'b T, barrier: &'b Barrier) -> LinkedListLocker<'b, T> {
        let mut current_ptr = entry.backward_link().load(Relaxed, barrier);
        loop {
            if current_ptr.tag() == Tag::First {
                std::thread::yield_now();
                current_ptr = entry.backward_link().load(Relaxed, barrier);
                continue;
            }
            if let Err((_, actual)) = entry.backward_link().compare_exchange(
                current_ptr,
                (current_ptr.try_into_arc(), Tag::First),
                Acquire,
                Relaxed,
            ) {
                current_ptr = actual;
                continue;
            }
            break;
        }
        LinkedListLocker { entry }
    }

    /// Locks the next entry and self.
    fn lock_next_self(
        entry: &'b T,
        barrier: &'b Barrier,
    ) -> (Option<LinkedListLocker<'b, T>>, LinkedListLocker<'b, T>) {
        loop {
            // Locks the next entry.
            let next_entry_ptr = entry.forward_link().load(Relaxed, barrier);
            let mut next_entry_locker = None;
            if let Some(next_entry_ref) = next_entry_ptr.as_ref() {
                next_entry_locker.replace(LinkedListLocker::lock(next_entry_ref, barrier));
            }

            #[allow(clippy::blocks_in_if_conditions)]
            if next_entry_locker.as_ref().map_or_else(
                || false,
                |locker| {
                    locker.entry.backward_link().load(Relaxed, barrier).as_raw()
                        != entry as *const _
                },
            ) {
                // The link has changed in the meantime.
                continue;
            }

            // Locks the next entry.
            //
            // Reading backward_link needs to be an acquire fence to correctly read forward_link.
            let mut current_ptr = entry.backward_link().load(Acquire, barrier);
            loop {
                if current_ptr.tag() == Tag::First {
                    // Currently, locked.
                    std::thread::yield_now();
                    current_ptr = entry.backward_link().load(Acquire, barrier);
                    continue;
                }
                if next_entry_ptr != entry.forward_link().load(Relaxed, barrier) {
                    // Pointer changed with the known next entry locked.
                    break;
                }

                if let Err((_, actual)) = entry.backward_link().compare_exchange(
                    current_ptr,
                    (current_ptr.try_into_arc(), Tag::First),
                    Acquire,
                    Relaxed,
                ) {
                    current_ptr = actual;
                    continue;
                }

                debug_assert_eq!(
                    next_entry_locker.as_ref().map_or_else(
                        || entry as *const _,
                        |locker| locker.entry.backward_link().load(Relaxed, barrier).as_raw()
                    ),
                    entry as *const _
                );
                debug_assert!(next_entry_locker.as_ref().map_or_else(
                    || true,
                    |locker| locker.entry as *const _
                        == entry.forward_link().load(Relaxed, barrier).as_raw()
                ));

                return (next_entry_locker, LinkedListLocker { entry });
            }
        }
    }
}

impl<'b, T: LinkedList> Drop for LinkedListLocker<'b, T> {
    fn drop(&mut self) {
        self.entry.backward_link().set_tag(Tag::None, Release);
    }
}
