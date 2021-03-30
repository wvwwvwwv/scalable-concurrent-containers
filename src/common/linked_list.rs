use crossbeam_epoch::{Atomic, Guard, Shared};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// LinkedList is a self-referential doubly linked list.
///
/// The default implementation of push_back and pop_self functions relies on traditional locking mechanisms.
pub trait LinkedList: crossbeam_epoch::Pointable + Sized {
    /// Returns a reference to the forward link.
    ///
    /// The pointer value may be tagged if the caller of push_back or remove has given a tag.
    /// The tag is reset as soon as the pointer is replaced with a new one.
    fn forward_link(&self) -> &Atomic<Self>;

    /// Returns a reference to the backward link.
    ///
    /// The pointer value may be tagged 1 when the entry is being modified.
    fn backward_link(&self) -> &Atomic<Self>;

    /// Adds the given entries after self.
    ///
    /// The forward link pointer is tagged with the given tag to indicate a special state.
    fn push_back<'g>(&self, entries: &[Shared<'g, Self>], tag: usize, guard: &'g Guard) -> bool {
        if entries.is_empty() {
            return false;
        }

        // Locks the next and self.
        let mut lockers = LinkedListLocker::lock_next_self(self, guard);

        // Connects the given entries one another.
        let mut first_entry = None;
        let mut last_entry: Option<&Self> = None;
        for entry in entries.iter() {
            if entry.is_null() {
                continue;
            }
            let entry_ref = unsafe { entry.deref() };
            if first_entry.is_none() {
                first_entry.replace(entry_ref);
            }
            if let Some(prev_entry) = last_entry.take() {
                prev_entry
                    .forward_link()
                    .store(Shared::from(entry_ref as *const _), Relaxed);
                entry_ref
                    .backward_link()
                    .store(Shared::from(prev_entry as *const _), Relaxed);
            }
            last_entry.replace(entry_ref);
        }

        if first_entry.is_none() {
            return false;
        }

        // Connects the given entries with the next and self.
        last_entry.as_ref().unwrap().forward_link().store(
            self.forward_link().load(Relaxed, guard).with_tag(0),
            Relaxed,
        );
        first_entry
            .as_ref()
            .unwrap()
            .backward_link()
            .store(Shared::from(self as *const _), Relaxed);

        // Makes the next entry point to the last entry.
        if let Some(next_locker) = lockers.0.as_mut() {
            next_locker.entry.backward_link().store(
                Shared::from(last_entry.unwrap() as *const _).with_tag(1),
                Relaxed,
            );
        }

        // Makes everything visible.
        self.forward_link().store(
            Shared::from(first_entry.unwrap() as *const _).with_tag(tag),
            Release,
        );

        true
    }

    /// Removes itself from the linked list.
    fn pop_self(&self, tag: usize, guard: &Guard) {
        // Locks the next entry and self.
        let mut lockers = LinkedListLocker::lock_next_self(self, guard);

        // Locks the previous entry and modifies the linked list.
        let prev_shared = self.backward_link().load(Relaxed, guard).with_tag(0);
        if !prev_shared.is_null() {
            // Makes the prev entry point to the next entry.
            let prev_entry_locker = LinkedListLocker::lock(unsafe { prev_shared.deref() }, guard);
            debug_assert_eq!(
                prev_entry_locker
                    .entry
                    .forward_link()
                    .load(Relaxed, guard)
                    .as_raw(),
                self as *const _
            );
            if let Some(next_entry_locker) = lockers.0.as_mut() {
                next_entry_locker
                    .entry
                    .backward_link()
                    .store(prev_shared.with_tag(1), Relaxed);
            }
            prev_entry_locker.entry.forward_link().store(
                self.forward_link().load(Relaxed, guard).with_tag(tag),
                Release,
            );
        } else if let Some(next_entry_locker) = lockers.0.as_mut() {
            next_entry_locker
                .entry
                .backward_link()
                .store(prev_shared.with_tag(1), Relaxed);
        }
    }
}

/// Linked list locker.
struct LinkedListLocker<'g, T: LinkedList> {
    entry: &'g T,
    guard: &'g Guard,
}

impl<'g, T: LinkedList> LinkedListLocker<'g, T> {
    /// Locks a single entry.
    fn lock(entry: &'g T, guard: &'g Guard) -> LinkedListLocker<'g, T> {
        let mut current = entry.backward_link().load(Relaxed, guard);
        loop {
            if current.tag() == 1 {
                current = entry.backward_link().load(Relaxed, guard);
                std::thread::yield_now();
                continue;
            }
            if let Err(err) = entry.backward_link().compare_exchange(
                current,
                current.with_tag(1),
                Acquire,
                Relaxed,
                guard,
            ) {
                current = err.current;
                continue;
            }
            break;
        }
        LinkedListLocker { entry, guard }
    }

    /// Locks the next entry and self.
    fn lock_next_self(
        entry: &'g T,
        guard: &'g Guard,
    ) -> (Option<LinkedListLocker<'g, T>>, LinkedListLocker<'g, T>) {
        loop {
            // Locks the next entry.
            let next_entry_ptr = entry.forward_link().load(Relaxed, guard);
            let mut next_entry_locker = None;
            if !next_entry_ptr.is_null() {
                next_entry_locker.replace(LinkedListLocker::lock(
                    unsafe { next_entry_ptr.deref() },
                    guard,
                ));
            }

            if next_entry_locker.as_ref().map_or_else(
                || false,
                |locker| {
                    locker.entry.backward_link().load(Relaxed, guard).as_raw() != entry as *const _
                },
            ) {
                // The link has changed in the meantime.
                continue;
            }

            // Locks the next entry.
            //
            // Reading backward_link needs to be an acquire fence to correctly read forward_link.
            let mut current_state = entry.backward_link().load(Acquire, guard);
            loop {
                if current_state.tag() == 1 {
                    // Currently, locked.
                    current_state = entry.backward_link().load(Acquire, guard);
                    std::thread::yield_now();
                    continue;
                }
                if next_entry_ptr != entry.forward_link().load(Relaxed, guard) {
                    // Pointer changed with the known next entry locked.
                    break;
                }

                if let Err(err) = entry.backward_link().compare_exchange(
                    current_state,
                    current_state.with_tag(1),
                    Acquire,
                    Relaxed,
                    guard,
                ) {
                    current_state = err.current;
                    continue;
                }

                debug_assert_eq!(
                    next_entry_locker.as_ref().map_or_else(
                        || entry as *const _,
                        |locker| locker.entry.backward_link().load(Relaxed, guard).as_raw()
                    ),
                    entry as *const _
                );
                debug_assert!(next_entry_locker.as_ref().map_or_else(
                    || true,
                    |locker| locker.entry as *const _
                        == entry.forward_link().load(Relaxed, guard).as_raw()
                ));

                return (next_entry_locker, LinkedListLocker { entry, guard });
            }
        }
    }
}

impl<'g, T: LinkedList> Drop for LinkedListLocker<'g, T> {
    fn drop(&mut self) {
        let mut current = self.entry.backward_link().load(Relaxed, self.guard);
        loop {
            debug_assert_eq!(current.tag(), 1);
            if let Err(result) = self.entry.backward_link().compare_exchange(
                current,
                current.with_tag(0),
                Release,
                Relaxed,
                self.guard,
            ) {
                current = result.current;
                continue;
            }
            break;
        }
    }
}
