use super::leaf::{InsertResult, RemoveResult, Scanner, DIMENSION};
use super::Leaf;
use crate::ebr::{AtomicShared, Guard, Ptr, Shared, Tag};
use crate::exit_guard::ExitGuard;
use crate::wait_queue::{DeriveAsyncWait, WaitQueue};
use crate::LinkedList;
use std::borrow::Borrow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::ptr;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU8};

/// [`Tag::First`] indicates the corresponding node has retired.
pub const RETIRED: Tag = Tag::First;

/// [`Tag::Second`] indicates the corresponding node is locked.
pub const LOCKED: Tag = Tag::Second;

/// [`LeafNode`] contains a list of instances of `K, V` [`Leaf`].
///
/// The layout of a leaf node: `|ptr(entry array)/max(child keys)|...|ptr(entry array)|`
pub struct LeafNode<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    /// Children of the [`LeafNode`].
    children: Leaf<K, AtomicShared<Leaf<K, V>>>,

    /// A child [`Leaf`] that has no upper key bound.
    ///
    /// It stores the maximum key in the node, and key-value pairs are firstly pushed to this
    /// [`Leaf`] until split.
    unbounded_child: AtomicShared<Leaf<K, V>>,

    /// On-going split operation.
    split_op: StructuralChange<K, V>,

    /// The latch protecting the [`LeafNode`].
    latch: AtomicU8,

    /// `wait_queue` for `latch`.
    wait_queue: WaitQueue,
}

impl<K, V> LeafNode<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    /// Creates a new empty [`LeafNode`].
    #[inline]
    pub(super) fn new() -> LeafNode<K, V> {
        LeafNode {
            children: Leaf::new(),
            unbounded_child: AtomicShared::null(),
            split_op: StructuralChange::default(),
            latch: AtomicU8::new(Tag::None.into()),
            wait_queue: WaitQueue::default(),
        }
    }

    /// Returns `true` if the [`LeafNode`] has retired.
    #[inline]
    pub(super) fn retired(&self, mo: Ordering) -> bool {
        self.unbounded_child.tag(mo) == RETIRED
    }

    /// Searches for an entry associated with the given key.
    #[inline]
    pub(super) fn search<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: 'g + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let (child, metadata) = self.children.min_greater_equal(key);
            if let Some((_, child)) = child {
                if let Some(child) = child.load(Acquire, guard).as_ref() {
                    if self.children.validate(metadata) {
                        // Data race with split.
                        //  - Writer: start to insert an intermediate low key leaf.
                        //  - Reader: read the metadata not including the intermediate low key leaf.
                        //  - Writer: insert the intermediate low key leaf.
                        //  - Writer: replace the high key leaf pointer.
                        //  - Reader: read the new high key leaf pointer
                        // Consequently, the reader may miss keys in the low key leaf.
                        //
                        // Resolution: metadata validation.
                        return child.search(key);
                    }
                }

                // The child leaf must have been just removed.
                //
                // The `LeafNode` metadata is updated before a leaf is removed. This implies that
                // the new `metadata` will be different from the current `metadata`.
            } else {
                let unbounded_ptr = self.unbounded_child.load(Acquire, guard);
                if let Some(unbounded) = unbounded_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        return unbounded.search(key);
                    }
                } else {
                    return None;
                }
            }
        }
    }

    /// Returns the minimum key entry.
    #[inline]
    pub(super) fn min<'g>(&self, guard: &'g Guard) -> Option<Scanner<'g, K, V>> {
        loop {
            let mut scanner = Scanner::new(&self.children);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_ptr = child.load(Acquire, guard);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        return Some(Scanner::new(child));
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, guard);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if self.children.validate(metadata) {
                    return Some(Scanner::new(unbounded));
                }
                continue;
            }
            return None;
        }
    }

    /// Returns a [`Scanner`] pointing to an entry that is close enough to the entry with the
    /// maximum key among those keys smaller than the given key.
    ///
    /// Returns `None` if all the keys in the [`LeafNode`] is equal to or greater than the given
    /// key.
    #[inline]
    pub(super) fn max_le_appr<'g, Q>(&self, key: &Q, guard: &'g Guard) -> Option<Scanner<'g, K, V>>
    where
        K: 'g + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            if let Some(scanner) = Scanner::max_less(&self.children, key) {
                if let Some((_, child)) = scanner.get() {
                    if let Some(child) = child.load(Acquire, guard).as_ref() {
                        if self.children.validate(scanner.metadata()) {
                            // Data race resolution - see `LeafNode::search`.
                            if let Some(scanner) = Scanner::max_less(child, key) {
                                return Some(scanner);
                            }
                            // Fallback.
                            break;
                        }
                    }
                    // It is not a hot loop - see `LeafNode::search`.
                    continue;
                }
            }
            // Fallback.
            break;
        }

        // Starts scanning from the minimum key.
        let mut min_scanner = self.min(guard)?;
        min_scanner.next();
        loop {
            if let Some((k, _)) = min_scanner.get() {
                if k.borrow() <= key {
                    return Some(min_scanner);
                }
                break;
            }
            min_scanner = min_scanner.jump(None, guard)?;
        }

        None
    }

    /// Inserts a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required.
    #[inline]
    pub(super) fn insert<D: DeriveAsyncWait>(
        &self,
        mut key: K,
        mut val: V,
        async_wait: &mut D,
        guard: &Guard,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        loop {
            let (child, metadata) = self.children.min_greater_equal(&key);
            if let Some((child_key, child)) = child {
                let child_ptr = child.load(Acquire, guard);
                if let Some(child_ref) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let insert_result = child_ref.insert(key, val);
                        match insert_result {
                            InsertResult::Success
                            | InsertResult::Duplicate(..)
                            | InsertResult::Retry(..) => return Ok(insert_result),
                            InsertResult::Full(k, v) | InsertResult::Retired(k, v) => {
                                let split_result = self.split_leaf(
                                    k,
                                    v,
                                    Some(child_key),
                                    child_ptr,
                                    child,
                                    async_wait,
                                    guard,
                                )?;
                                if let InsertResult::Retry(k, v) = split_result {
                                    key = k;
                                    val = v;
                                    continue;
                                }
                                return Ok(split_result);
                            }
                            InsertResult::Frozen(k, v) => {
                                // The `Leaf` is being split: retry.
                                self.wait(async_wait);
                                return Err((k, v));
                            }
                        };
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }

            let mut unbounded_ptr = self.unbounded_child.load(Acquire, guard);
            if unbounded_ptr.is_null() {
                match self.unbounded_child.compare_exchange(
                    Ptr::null(),
                    (Some(Shared::new(Leaf::new())), Tag::None),
                    AcqRel,
                    Acquire,
                    guard,
                ) {
                    Ok((_, ptr)) => {
                        unbounded_ptr = ptr;
                    }
                    Err((_, actual)) => {
                        unbounded_ptr = actual;
                    }
                }
            }
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !self.children.validate(metadata) {
                    continue;
                }
                let insert_result = unbounded.insert(key, val);
                match insert_result {
                    InsertResult::Success
                    | InsertResult::Duplicate(..)
                    | InsertResult::Retry(..) => return Ok(insert_result),
                    InsertResult::Full(k, v) | InsertResult::Retired(k, v) => {
                        let split_result = self.split_leaf(
                            k,
                            v,
                            None,
                            unbounded_ptr,
                            &self.unbounded_child,
                            async_wait,
                            guard,
                        )?;
                        if let InsertResult::Retry(k, v) = split_result {
                            key = k;
                            val = v;
                            continue;
                        }
                        return Ok(split_result);
                    }
                    InsertResult::Frozen(k, v) => {
                        self.wait(async_wait);
                        return Err((k, v));
                    }
                };
            }
            return Ok(InsertResult::Retired(key, val));
        }
    }

    /// Removes an entry associated with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required with a Boolean flag indicating that an entry has been removed.
    #[inline]
    pub(super) fn remove_if<Q, F: FnMut(&V) -> bool, D: DeriveAsyncWait>(
        &self,
        key: &Q,
        condition: &mut F,
        async_wait: &mut D,
        guard: &Guard,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let (child, metadata) = self.children.min_greater_equal(key);
            if let Some((_, child)) = child {
                let child_ptr = child.load(Acquire, guard);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let result = child.remove_if(key, condition);
                        if result == RemoveResult::Frozen {
                            // When a `Leaf` is frozen, its entries may be being copied to new
                            // `Leaves`.
                            self.wait(async_wait);
                            return Err(false);
                        } else if result == RemoveResult::Retired {
                            return Ok(self.coalesce(guard));
                        }
                        return Ok(result);
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, guard);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                let result = unbounded.remove_if(key, condition);
                if result == RemoveResult::Frozen {
                    self.wait(async_wait);
                    return Err(false);
                } else if result == RemoveResult::Retired {
                    return Ok(self.coalesce(guard));
                }
                return Ok(result);
            }
            return Ok(RemoveResult::Fail);
        }
    }

    /// Splits itself into the given leaf nodes, and returns the middle key value.
    #[allow(clippy::too_many_lines)]
    pub(super) fn split_leaf_node<'g>(
        &'g self,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        guard: &'g Guard,
    ) -> &'g K {
        let mut middle_key = None;

        low_key_leaf_node.latch.store(LOCKED.into(), Relaxed);
        high_key_leaf_node.latch.store(LOCKED.into(), Relaxed);

        // It is safe to keep the pointers to the new leaf nodes in this full leaf node since the
        // whole split operation is protected under a single `ebr::Guard`, and the pointers are
        // only dereferenced during the operation.
        self.split_op.low_key_leaf_node.swap(
            (low_key_leaf_node as *const LeafNode<K, V>).cast_mut(),
            Relaxed,
        );
        self.split_op.high_key_leaf_node.swap(
            (high_key_leaf_node as *const LeafNode<K, V>).cast_mut(),
            Relaxed,
        );

        // Builds a list of valid leaves
        #[allow(clippy::type_complexity)]
        let mut entry_array: [Option<(Option<&K>, AtomicShared<Leaf<K, V>>)>;
            DIMENSION.num_entries + 2] = Default::default();
        let mut num_entries = 0;
        let low_key_leaf_ref = self
            .split_op
            .low_key_leaf
            .load(Relaxed, guard)
            .as_ref()
            .unwrap();
        let middle_key_ref = low_key_leaf_ref.max_key().unwrap();
        let scanner = Scanner::new(&self.children);
        let recommended_boundary = Leaf::<K, V>::optimal_boundary(scanner.metadata());
        for entry in scanner {
            if unsafe {
                self.split_op
                    .origin_leaf_key
                    .load(Relaxed)
                    .as_ref()
                    .map_or_else(|| false, |key| entry.0 == key)
            } {
                entry_array[num_entries].replace((
                    Some(middle_key_ref),
                    self.split_op.low_key_leaf.clone(Relaxed, guard),
                ));
                num_entries += 1;
                if !self.split_op.high_key_leaf.load(Relaxed, guard).is_null() {
                    entry_array[num_entries].replace((
                        Some(entry.0),
                        self.split_op.high_key_leaf.clone(Relaxed, guard),
                    ));
                    num_entries += 1;
                }
            } else {
                entry_array[num_entries].replace((Some(entry.0), entry.1.clone(Relaxed, guard)));
                num_entries += 1;
            }
        }

        if self.split_op.origin_leaf_key.load(Relaxed).is_null() {
            // If the origin is an unbounded node, assign the high key node to the high key node's
            // unbounded.
            entry_array[num_entries].replace((
                Some(middle_key_ref),
                self.split_op.low_key_leaf.clone(Relaxed, guard),
            ));
            num_entries += 1;
            if !self.split_op.high_key_leaf.load(Relaxed, guard).is_null() {
                entry_array[num_entries]
                    .replace((None, self.split_op.high_key_leaf.clone(Relaxed, guard)));
                num_entries += 1;
            }
        } else {
            // If the origin is a bounded node, assign the unbounded leaf as the high key node's
            // unbounded.
            entry_array[num_entries].replace((None, self.unbounded_child.clone(Relaxed, guard)));
            num_entries += 1;
        }
        debug_assert!(num_entries >= 2);

        let low_key_leaf_array_size = recommended_boundary.min(num_entries - 1);
        for (i, entry) in entry_array.iter().enumerate() {
            if let Some((k, v)) = entry {
                match (i + 1).cmp(&low_key_leaf_array_size) {
                    Less => {
                        low_key_leaf_node.children.insert_unchecked(
                            k.unwrap().clone(),
                            v.clone(Relaxed, guard),
                            i,
                        );
                    }
                    Equal => {
                        middle_key.replace(k.unwrap());
                        low_key_leaf_node
                            .unbounded_child
                            .swap((v.get_shared(Relaxed, guard), Tag::None), Relaxed);
                    }
                    Greater => {
                        if let Some(k) = k.cloned() {
                            high_key_leaf_node.children.insert_unchecked(
                                k,
                                v.clone(Relaxed, guard),
                                i - low_key_leaf_array_size,
                            );
                        } else {
                            high_key_leaf_node
                                .unbounded_child
                                .swap((v.get_shared(Relaxed, guard), Tag::None), Relaxed);
                        }
                    }
                }
            } else {
                break;
            }
        }

        middle_key.unwrap()
    }

    /// Commits an on-going structural change.
    #[inline]
    pub(super) fn commit(&self, guard: &Guard) {
        // Unfreeze both leaves.
        if let Some(origin_leaf) = self.split_op.origin_leaf.swap((None, Tag::None), Relaxed).0 {
            // Make the origin leaf unreachable before making the new leaves updatable.
            origin_leaf.delete_self(Relaxed);
            let _: bool = origin_leaf.release(guard);
        }
        let low_key_leaf = self
            .split_op
            .low_key_leaf
            .load(Relaxed, guard)
            .as_ref()
            .unwrap();
        let high_key_leaf = self
            .split_op
            .high_key_leaf
            .load(Relaxed, guard)
            .as_ref()
            .unwrap();
        let unfrozen = low_key_leaf.thaw();
        debug_assert!(unfrozen);
        let unfrozen = high_key_leaf.thaw();
        debug_assert!(unfrozen);

        // It is safe to dereference those pointers since the whole split operation is under a
        // single `ebr::Guard`.
        if let Some(low_key_leaf_node) =
            unsafe { self.split_op.low_key_leaf_node.load(Relaxed).as_ref() }
        {
            let origin = low_key_leaf_node.split_op.reset();
            low_key_leaf_node.unlock();
            origin.map(|o| o.release(guard));
        }

        if let Some(high_key_leaf_node) =
            unsafe { self.split_op.high_key_leaf_node.load(Relaxed).as_ref() }
        {
            let origin = high_key_leaf_node.split_op.reset();
            high_key_leaf_node.unlock();
            origin.map(|o| o.release(guard));
        }

        let origin = self.split_op.reset();
        self.retire();
        origin.map(|o| o.release(guard));
    }

    /// Rolls back the ongoing split operation.
    #[inline]
    pub(super) fn rollback(&self, guard: &Guard) {
        let low_key_leaf = self
            .split_op
            .low_key_leaf
            .load(Relaxed, guard)
            .as_ref()
            .unwrap();
        let high_key_leaf = self
            .split_op
            .high_key_leaf
            .load(Relaxed, guard)
            .as_ref()
            .unwrap();

        // Roll back the linked list state.
        //
        // `high_key_leaf` must be deleted first in order for `Scanners` not to omit entries.
        let deleted = high_key_leaf.delete_self(Relaxed);
        debug_assert!(deleted);
        let deleted = low_key_leaf.delete_self(Relaxed);
        debug_assert!(deleted);

        if let Some(origin_leaf) = self.split_op.origin_leaf.swap((None, Tag::None), Relaxed).0 {
            // Unfreeze the origin.
            let unfrozen = origin_leaf.thaw();
            debug_assert!(unfrozen);

            // Remove the mark from the full leaf node.
            let unmarked = origin_leaf.unmark(Release);
            debug_assert!(unmarked);
        }

        let origin = self.split_op.reset();
        self.unlock();
        origin.map(|o| o.release(guard));
    }

    /// Cleans up logically deleted [`LeafNode`] instances in the linked list.
    ///
    /// If the target leaf node does not exist in the sub-tree, returns `false`.
    #[inline]
    pub(super) fn cleanup_link<'g, Q>(&self, key: &Q, tranverse_max: bool, guard: &'g Guard) -> bool
    where
        K: 'g + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let scanner = if tranverse_max {
            if let Some(unbounded) = self.unbounded_child.load(Acquire, guard).as_ref() {
                Scanner::new(unbounded)
            } else {
                return false;
            }
        } else if let Some(leaf_scanner) = Scanner::max_less(&self.children, key) {
            if let Some((_, child)) = leaf_scanner.get() {
                if let Some(child) = child.load(Acquire, guard).as_ref() {
                    Scanner::new(child)
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        };

        // It *would* be the maximum leaf node among those that containing keys smaller than the
        // target key. Hopefully, two jumps will be sufficient.
        scanner.jump(None, guard).map(|s| s.jump(None, guard));
        true
    }

    /// Waits for the lock on the [`LeafNode`] to be released.
    #[inline]
    pub(super) fn wait<D: DeriveAsyncWait>(&self, async_wait: &mut D) {
        let waiter = || {
            if self.latch.load(Relaxed) == LOCKED.into() {
                // The `LeafNode` is being split or locked.
                return Err(());
            }
            Ok(())
        };

        if let Some(async_wait) = async_wait.derive() {
            let _result = self.wait_queue.push_async_entry(async_wait, waiter);
        } else {
            let _result = self.wait_queue.wait_sync(waiter);
        }
    }

    /// Splits a full leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn split_leaf<D: DeriveAsyncWait>(
        &self,
        key: K,
        val: V,
        full_leaf_key: Option<&K>,
        full_leaf_ptr: Ptr<Leaf<K, V>>,
        full_leaf: &AtomicShared<Leaf<K, V>>,
        async_wait: &mut D,
        guard: &Guard,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        if !self.try_lock() {
            self.wait(async_wait);
            return Err((key, val));
        }
        if self.retired(Relaxed) {
            self.unlock();
            return Ok(InsertResult::Retired(key, val));
        }
        if full_leaf_ptr != full_leaf.load(Relaxed, guard) {
            self.unlock();
            return Err((key, val));
        }

        let prev = self
            .split_op
            .origin_leaf
            .swap((full_leaf.get_shared(Relaxed, guard), Tag::None), Relaxed)
            .0;
        debug_assert!(prev.is_none());

        if let Some(full_leaf_key) = full_leaf_key {
            self.split_op
                .origin_leaf_key
                .store((full_leaf_key as *const K).cast_mut(), Relaxed);
        }

        let target = full_leaf_ptr.as_ref().unwrap();
        let mut low_key_leaf_arc = None;
        let mut high_key_leaf_arc = None;

        // Distribute entries to two leaves after make the target retired.
        let mut exit_guard = ExitGuard::new(true, |rollback| {
            if rollback {
                target.thaw();
                self.split_op.reset();
                self.unlock();
            }
        });
        target.freeze_and_distribute(&mut low_key_leaf_arc, &mut high_key_leaf_arc);

        if let Some(low_key_leaf) = low_key_leaf_arc.take() {
            self.split_op
                .low_key_leaf
                .swap((Some(low_key_leaf), Tag::None), Relaxed);
            if let Some(high_key_leaf) = high_key_leaf_arc.take() {
                self.split_op
                    .high_key_leaf
                    .swap((Some(high_key_leaf), Tag::None), Relaxed);
            }
        } else {
            // No valid keys in the full leaf.
            self.split_op
                .low_key_leaf
                .swap((Some(Shared::new(Leaf::new())), Tag::None), Relaxed);
        }

        // When a new leaf is added to the linked list, the leaf is marked to let `Scanners`
        // acknowledge that the newly added leaf may contain keys that are smaller than those
        // having been `scanned`.
        let low_key_leaf_ptr = self.split_op.low_key_leaf.load(Relaxed, guard);
        let high_key_leaf_ptr = self.split_op.high_key_leaf.load(Relaxed, guard);
        let unused_leaf = if let Some(high_key_leaf) = high_key_leaf_ptr.as_ref() {
            // From here, `Scanners` can reach the new leaves.
            let result = target.push_back(
                high_key_leaf_ptr.get_shared().unwrap(),
                true,
                Release,
                guard,
            );
            debug_assert!(result.is_ok());
            let result =
                target.push_back(low_key_leaf_ptr.get_shared().unwrap(), true, Release, guard);
            debug_assert!(result.is_ok());

            // Take the max key value stored in the low key leaf as the leaf key.
            let low_key_leaf = low_key_leaf_ptr.as_ref().unwrap();
            let max_key = low_key_leaf.max_key().unwrap();

            // Need to freeze the leaf before trying to make it reachable.
            let frozen = low_key_leaf.freeze();
            debug_assert!(frozen);

            match self.children.insert(
                max_key.clone(),
                self.split_op.low_key_leaf.clone(Relaxed, guard),
            ) {
                InsertResult::Success => (),
                InsertResult::Duplicate(..)
                | InsertResult::Frozen(..)
                | InsertResult::Retry(..) => unreachable!(),
                InsertResult::Full(_, _) | InsertResult::Retired(_, _) => {
                    // Need to freeze the other leaf.
                    let frozen = high_key_leaf.freeze();
                    debug_assert!(frozen);
                    *exit_guard = false;
                    return Ok(InsertResult::Full(key, val));
                }
            };

            // Mark the full leaf deleted before making the new one reachable and updatable.
            //
            // If the order is reversed, there emerges a possibility that entries were removed from
            // the replaced leaf node whereas those entries still remain in `unused_leaf`; if that
            // happens, iterators may see the removed entries momentarily.
            let delete = target.delete_self(Release);
            debug_assert!(delete);

            // Unfreeze the leaf.
            let unfrozen = low_key_leaf.thaw();
            debug_assert!(unfrozen);

            // Replace the full leaf with the high-key leaf.
            let high_key_leaf = self
                .split_op
                .high_key_leaf
                .swap((None, Tag::None), Relaxed)
                .0;
            full_leaf.swap((high_key_leaf, Tag::None), Release).0
        } else {
            // From here, `Scanners` can reach the new leaf.
            //
            // The full leaf is marked so that readers know that the next leaves may contain
            // smaller keys.
            let result =
                target.push_back(low_key_leaf_ptr.get_shared().unwrap(), true, Release, guard);
            debug_assert!(result.is_ok());

            // Mark the full leaf deleted before making the new one reachable and updatable.
            let deleted = target.delete_self(Release);
            debug_assert!(deleted);

            full_leaf
                .swap(
                    self.split_op.low_key_leaf.swap((None, Tag::None), Release),
                    Release,
                )
                .0
        };
        *exit_guard = false;

        let origin = self.split_op.reset();
        self.unlock();
        origin.map(|o| o.release(guard));
        unused_leaf.map(|u| u.release(guard));

        // Since a new leaf has been inserted, the caller can retry.
        Ok(InsertResult::Retry(key, val))
    }

    /// Tries to coalesce empty or obsolete leaves after a successful removal of an entry.
    fn coalesce<Q>(&self, guard: &Guard) -> RemoveResult
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut uncleaned_leaf = false;
        let mut prev_valid_leaf = None;
        while let Some(lock) = Locker::try_lock(self) {
            prev_valid_leaf.take();
            for entry in Scanner::new(&self.children) {
                let leaf_ptr = entry.1.load(Relaxed, guard);
                let leaf = leaf_ptr.as_ref().unwrap();
                if leaf.is_retired() {
                    let deleted = leaf.delete_self(Relaxed);
                    debug_assert!(deleted);
                    let result = self.children.remove_if(entry.0.borrow(), &mut |_| true);
                    debug_assert_ne!(result, RemoveResult::Fail);

                    // The pointer is nullified after the metadata of `self.children` is updated so
                    // that readers are able to retry when they find it being `null`.
                    if let Some(leaf) = entry.1.swap((None, Tag::None), Release).0 {
                        let _: bool = leaf.release(guard);
                        if let Some(prev_leaf) = prev_valid_leaf.as_ref() {
                            // One jump is sufficient.
                            Scanner::new(*prev_leaf).jump(None, guard);
                        } else {
                            uncleaned_leaf = true;
                        }
                    }
                } else {
                    prev_valid_leaf.replace(leaf);
                }
            }

            // The unbounded leaf becomes unreachable after all the other leaves are gone.
            let fully_empty = if prev_valid_leaf.is_some() {
                false
            } else {
                let unbounded_ptr = self.unbounded_child.load(Relaxed, guard);
                if let Some(unbounded) = unbounded_ptr.as_ref() {
                    if unbounded.is_retired() {
                        let deleted = unbounded.delete_self(Relaxed);
                        debug_assert!(deleted);

                        // It has to mark the pointer in order to prevent `LeafNode::insert` from
                        // replacing it with a new `Leaf`.
                        if let Some(obsolete_leaf) =
                            self.unbounded_child.swap((None, RETIRED), Release).0
                        {
                            let _: bool = obsolete_leaf.release(guard);
                            uncleaned_leaf = true;
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    debug_assert!(unbounded_ptr.tag() == RETIRED);
                    true
                }
            };

            if fully_empty {
                return RemoveResult::Retired;
            }

            drop(lock);
            if !self.has_retired_leaf(guard) {
                break;
            }
        }

        if uncleaned_leaf {
            RemoveResult::Cleanup
        } else {
            RemoveResult::Success
        }
    }

    /// Checks if the [`LeafNode`] has a retired [`Leaf`].
    fn has_retired_leaf(&self, guard: &Guard) -> bool {
        let mut has_valid_leaf = false;
        for entry in Scanner::new(&self.children) {
            let leaf_ptr = entry.1.load(Relaxed, guard);
            if let Some(leaf) = leaf_ptr.as_ref() {
                if leaf.is_retired() {
                    return true;
                }
                has_valid_leaf = true;
            }
        }
        if !has_valid_leaf {
            let unbounded_ptr = self.unbounded_child.load(Relaxed, guard);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.is_retired() {
                    return true;
                }
            }
        }
        false
    }

    /// Tries to lock the [`LeafNode`].
    fn try_lock(&self) -> bool {
        self.latch
            .compare_exchange(Tag::None.into(), LOCKED.into(), Acquire, Relaxed)
            .is_ok()
    }

    /// Unlocks the [`LeafNode`].
    fn unlock(&self) {
        debug_assert_eq!(self.latch.load(Relaxed), LOCKED.into());
        self.latch.store(Tag::None.into(), Release);
        self.wait_queue.signal();
    }

    /// Retires itself.
    fn retire(&self) {
        debug_assert_eq!(self.latch.load(Relaxed), LOCKED.into());
        self.latch.store(RETIRED.into(), Release);
        self.wait_queue.signal();
    }
}

/// [`Locker`] holds exclusive access to a [`Leaf`].
pub struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    leaf_node: &'n LeafNode<K, V>,
}

impl<'n, K, V> Locker<'n, K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    /// Acquires exclusive lock on the [`LeafNode`].
    #[inline]
    pub(super) fn try_lock(leaf_node: &'n LeafNode<K, V>) -> Option<Locker<'n, K, V>> {
        if leaf_node.try_lock() {
            Some(Locker { leaf_node })
        } else {
            None
        }
    }
}

impl<'n, K, V> Drop for Locker<'n, K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    #[inline]
    fn drop(&mut self) {
        self.leaf_node.unlock();
    }
}

/// [`StructuralChange`] stores intermediate results during a split operation.
///
/// `AtomicPtr` members may point to values under the protection of the [`Guard`] used for the
/// split operation.
pub struct StructuralChange<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    origin_leaf_key: AtomicPtr<K>,
    origin_leaf: AtomicShared<Leaf<K, V>>,
    low_key_leaf: AtomicShared<Leaf<K, V>>,
    high_key_leaf: AtomicShared<Leaf<K, V>>,
    low_key_leaf_node: AtomicPtr<LeafNode<K, V>>,
    high_key_leaf_node: AtomicPtr<LeafNode<K, V>>,
}

impl<K, V> StructuralChange<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    fn reset(&self) -> Option<Shared<Leaf<K, V>>> {
        self.origin_leaf_key.store(ptr::null_mut(), Relaxed);
        self.low_key_leaf.swap((None, Tag::None), Relaxed);
        self.high_key_leaf.swap((None, Tag::None), Relaxed);
        self.low_key_leaf_node.store(ptr::null_mut(), Relaxed);
        self.high_key_leaf_node.store(ptr::null_mut(), Relaxed);
        self.origin_leaf.swap((None, Tag::None), Relaxed).0
    }
}

impl<K, V> Default for StructuralChange<K, V>
where
    K: 'static + Clone + Ord,
    V: 'static + Clone,
{
    #[inline]
    fn default() -> Self {
        Self {
            origin_leaf_key: AtomicPtr::default(),
            origin_leaf: AtomicShared::null(),
            low_key_leaf: AtomicShared::null(),
            high_key_leaf: AtomicShared::null(),
            low_key_leaf_node: AtomicPtr::default(),
            high_key_leaf_node: AtomicPtr::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;
    use tokio::sync::Barrier;

    #[cfg_attr(miri, ignore)]
    #[test]
    fn basic() {
        let guard = Guard::new();
        let leaf_node: LeafNode<String, String> = LeafNode::new();
        assert!(matches!(
            leaf_node.insert(
                "MY GOODNESS!".to_owned(),
                "OH MY GOD!!".to_owned(),
                &mut (),
                &guard
            ),
            Ok(InsertResult::Success)
        ));
        assert!(matches!(
            leaf_node.insert(
                "GOOD DAY".to_owned(),
                "OH MY GOD!!".to_owned(),
                &mut (),
                &guard
            ),
            Ok(InsertResult::Success)
        ));
        assert_eq!(
            leaf_node.search("MY GOODNESS!", &guard).unwrap(),
            "OH MY GOD!!"
        );
        assert_eq!(leaf_node.search("GOOD DAY", &guard).unwrap(), "OH MY GOD!!");
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("GOOD DAY", &mut |v| v == "OH MY", &mut (), &guard),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>(
                "GOOD DAY",
                &mut |v| v == "OH MY GOD!!",
                &mut (),
                &guard
            ),
            Ok(RemoveResult::Success)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("GOOD", &mut |v| v == "OH MY", &mut (), &guard),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("MY GOODNESS!", &mut |_| true, &mut (), &guard),
            Ok(RemoveResult::Retired)
        ));
        assert!(matches!(
            leaf_node.insert("HI".to_owned(), "HO".to_owned(), &mut (), &guard),
            Ok(InsertResult::Retired(..))
        ));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn bulk() {
        let guard = Guard::new();
        let leaf_node: LeafNode<usize, usize> = LeafNode::new();
        for k in 0..1024 {
            let mut result = leaf_node.insert(k, k, &mut (), &guard);
            if result.is_err() {
                result = leaf_node.insert(k, k, &mut (), &guard);
            }
            match result.unwrap() {
                InsertResult::Success => {
                    assert_eq!(leaf_node.search(&k, &guard), Some(&k));
                    continue;
                }
                InsertResult::Duplicate(..)
                | InsertResult::Frozen(..)
                | InsertResult::Retired(..) => unreachable!(),
                InsertResult::Full(_, _) => {
                    leaf_node.rollback(&guard);
                    for r in 0..(k - 1) {
                        assert_eq!(leaf_node.search(&r, &guard), Some(&r));
                        assert!(leaf_node
                            .remove_if::<_, _, _>(&r, &mut |_| true, &mut (), &guard)
                            .is_ok());
                        assert_eq!(leaf_node.search(&r, &guard), None);
                    }
                    assert_eq!(leaf_node.search(&(k - 1), &guard), Some(&(k - 1)));
                    assert_eq!(
                        leaf_node.remove_if::<_, _, _>(&(k - 1), &mut |_| true, &mut (), &guard),
                        Ok(RemoveResult::Retired)
                    );
                    assert_eq!(leaf_node.search(&(k - 1), &guard), None);
                    break;
                }
                InsertResult::Retry(..) => {
                    assert!(leaf_node.insert(k, k, &mut (), &guard).is_ok());
                }
            }
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel() {
        let num_tasks = 8;
        let workload_size = 64;
        let barrier = Shared::new(Barrier::new(num_tasks));
        for _ in 0..16 {
            let leaf_node = Shared::new(LeafNode::new());
            assert!(leaf_node
                .insert(usize::MAX, usize::MAX, &mut (), &Guard::new())
                .is_ok());
            let mut task_handles = Vec::with_capacity(num_tasks);
            for task_id in 0..num_tasks {
                let barrier_clone = barrier.clone();
                let leaf_node_clone = leaf_node.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_clone.wait().await;
                    let guard = Guard::new();
                    let mut max_key = None;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        loop {
                            if let Ok(r) = leaf_node_clone.insert(id, id, &mut (), &guard) {
                                match r {
                                    InsertResult::Success => {
                                        match leaf_node_clone.insert(id, id, &mut (), &guard) {
                                            Ok(InsertResult::Duplicate(..)) | Err(_) => (),
                                            _ => unreachable!(),
                                        }
                                        break;
                                    }
                                    InsertResult::Full(..) => {
                                        leaf_node_clone.rollback(&guard);
                                        max_key.replace(id);
                                        break;
                                    }
                                    InsertResult::Frozen(..) | InsertResult::Retry(..) => {
                                        continue;
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                        if max_key.is_some() {
                            break;
                        }
                    }
                    for id in range.clone() {
                        if max_key.map_or(false, |m| m == id) {
                            break;
                        }
                        assert_eq!(leaf_node_clone.search(&id, &guard), Some(&id));
                    }
                    for id in range {
                        if max_key.map_or(false, |m| m == id) {
                            break;
                        }
                        let mut removed = false;
                        loop {
                            match leaf_node_clone.remove_if::<_, _, _>(
                                &id,
                                &mut |_| true,
                                &mut (),
                                &guard,
                            ) {
                                Ok(r) => match r {
                                    RemoveResult::Success | RemoveResult::Cleanup => break,
                                    RemoveResult::Fail => {
                                        assert!(removed);
                                        break;
                                    }
                                    RemoveResult::Frozen | RemoveResult::Retired => unreachable!(),
                                },
                                Err(r) => removed |= r,
                            }
                        }
                        assert!(leaf_node_clone.search(&id, &guard).is_none(), "{}", id);
                        if let Ok(RemoveResult::Success) = leaf_node_clone.remove_if::<_, _, _>(
                            &id,
                            &mut |_| true,
                            &mut (),
                            &guard,
                        ) {
                            unreachable!()
                        }
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert!(leaf_node
                .remove_if::<_, _, _>(&usize::MAX, &mut |_| true, &mut (), &Guard::new())
                .is_ok());
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 16_usize;
        let workload_size = 64_usize;
        for _ in 0..16 {
            for k in 0..=workload_size {
                let barrier = Shared::new(Barrier::new(num_tasks));
                let leaf_node: Shared<LeafNode<usize, usize>> = Shared::new(LeafNode::new());
                let inserted: Shared<AtomicBool> = Shared::new(AtomicBool::new(false));
                let mut task_handles = Vec::with_capacity(num_tasks);
                for _ in 0..num_tasks {
                    let barrier_clone = barrier.clone();
                    let leaf_node_clone = leaf_node.clone();
                    let inserted_clone = inserted.clone();
                    task_handles.push(tokio::spawn(async move {
                        {
                            barrier_clone.wait().await;
                            let guard = Guard::new();
                            match leaf_node_clone.insert(k, k, &mut (), &guard) {
                                Ok(InsertResult::Success) => {
                                    assert!(!inserted_clone.swap(true, Relaxed));
                                }
                                Ok(InsertResult::Full(_, _) | InsertResult::Retired(_, _)) => {
                                    leaf_node_clone.rollback(&guard);
                                }
                                _ => (),
                            };
                        }
                        {
                            barrier_clone.wait().await;
                            let guard = Guard::new();
                            for i in 0..workload_size {
                                if i != k {
                                    if let Ok(
                                        InsertResult::Full(_, _) | InsertResult::Retired(_, _),
                                    ) = leaf_node_clone.insert(i, i, &mut (), &guard)
                                    {
                                        leaf_node_clone.rollback(&guard);
                                    }
                                }
                                assert_eq!(leaf_node_clone.search(&k, &guard).unwrap(), &k);
                            }
                            for i in 0..workload_size {
                                let max_scanner = leaf_node_clone.max_le_appr(&k, &guard).unwrap();
                                assert!(*max_scanner.get().unwrap().0 <= k);
                                let mut min_scanner = leaf_node_clone.min(&guard).unwrap();
                                if let Some((k_ref, v_ref)) = min_scanner.next() {
                                    assert_eq!(*k_ref, *v_ref);
                                    assert!(*k_ref <= k);
                                } else {
                                    let (k_ref, v_ref) =
                                        min_scanner.jump(None, &guard).unwrap().get().unwrap();
                                    assert_eq!(*k_ref, *v_ref);
                                    assert!(*k_ref <= k);
                                }
                                let _result = leaf_node_clone.remove_if::<_, _, _>(
                                    &i,
                                    &mut |v| *v != k,
                                    &mut (),
                                    &guard,
                                );
                                assert_eq!(leaf_node_clone.search(&k, &guard).unwrap(), &k);
                            }
                        }
                    }));
                }
                for r in futures::future::join_all(task_handles).await {
                    assert!(r.is_ok());
                }
                assert!(inserted.load(Relaxed));
            }
        }
    }
}
