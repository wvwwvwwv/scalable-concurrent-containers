use super::leaf::{InsertResult, RemoveResult, Scanner, DIMENSION};
use super::Leaf;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::{DeriveAsyncWait, WaitQueue};
use crate::LinkedList;

use std::borrow::Borrow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::ptr::addr_of;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};

/// [`Tag::First`] indicates the corresponding node has retired.
pub const RETIRED: Tag = Tag::First;

/// [`Tag::Second`] indicates the corresponding node is locked.
pub const LOCKED: Tag = Tag::Second;

/// [`LeafNode`] contains a list of instances of `K, V` [`Leaf`].
///
/// The layout of a leaf node: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
pub struct LeafNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Children of the [`LeafNode`].
    children: Leaf<K, AtomicArc<Leaf<K, V>>>,

    /// A child [`Leaf`] that has no upper key bound.
    ///
    /// It stores the maximum key in the node, and key-value pairs are firstly pushed to this
    /// [`Leaf`].
    unbounded_child: AtomicArc<Leaf<K, V>>,

    /// `latch` acts as a mutex of the [`LeafNode`] that also stores the information about an
    /// on-going structural change.
    latch: AtomicArc<StructuralChange<K, V>>,

    /// `wait_queue` for `latch`.
    wait_queue: WaitQueue,
}

impl<K, V> LeafNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new empty [`LeafNode`].
    #[inline]
    pub(super) fn new() -> LeafNode<K, V> {
        LeafNode {
            children: Leaf::new(),
            unbounded_child: AtomicArc::null(),
            latch: AtomicArc::null(),
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
    pub(super) fn search<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<&'b V>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let (child, metadata) = self.children.min_greater_equal(key);
            if let Some((_, child)) = child {
                if let Some(child) = child.load(Acquire, barrier).as_ref() {
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
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if !self.children.validate(metadata) {
                    continue;
                }
                return unbounded.search(key);
            }
            return None;
        }
    }

    /// Returns the minimum key entry.
    #[inline]
    pub(super) fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
        loop {
            let mut scanner = Scanner::new(&self.children);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        return Some(Scanner::new(child));
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
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
    /// It returns `None` if all the keys in the [`LeafNode`] is equal to or greater than the given
    /// key.
    #[inline]
    pub(super) fn max_le_appr<'b, Q>(
        &self,
        key: &Q,
        barrier: &'b Barrier,
    ) -> Option<Scanner<'b, K, V>>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            if let Some(scanner) = Scanner::max_less(&self.children, key) {
                if let Some((_, child)) = scanner.get() {
                    if let Some(child) = child.load(Acquire, barrier).as_ref() {
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
        let mut min_scanner = self.min(barrier)?;
        min_scanner.next();
        loop {
            if let Some((k, _)) = min_scanner.get() {
                if k.borrow() <= key {
                    return Some(min_scanner);
                }
                break;
            }
            min_scanner = min_scanner.jump(None, barrier)?;
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
        mut value: V,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        loop {
            let (child, metadata) = self.children.min_greater_equal(&key);
            if let Some((child_key, child)) = child {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child_ref) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let insert_result = child_ref.insert(key, value);
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
                                    barrier,
                                )?;
                                if let InsertResult::Retry(k, v) = split_result {
                                    key = k;
                                    value = v;
                                    continue;
                                }
                                return Ok(split_result);
                            }
                            InsertResult::Frozen(k, v) => {
                                // The `Leaf` is being split: retry.
                                self.wait(async_wait, barrier);
                                return Err((k, v));
                            }
                        };
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }

            let mut unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if unbounded_ptr.is_null() {
                match self.unbounded_child.compare_exchange(
                    Ptr::null(),
                    (Some(Arc::new(Leaf::new())), Tag::None),
                    AcqRel,
                    Acquire,
                    barrier,
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
                let insert_result = unbounded.insert(key, value);
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
                            barrier,
                        )?;
                        if let InsertResult::Retry(k, v) = split_result {
                            key = k;
                            value = v;
                            continue;
                        }
                        return Ok(split_result);
                    }
                    InsertResult::Frozen(k, v) => {
                        self.wait(async_wait, barrier);
                        return Err((k, v));
                    }
                };
            }
            return Ok(InsertResult::Retired(key, value));
        }
    }

    /// Removes an entry associated with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required with a boolean flag indicating that an entry has been removed.
    #[inline]
    pub(super) fn remove_if<Q, F: FnMut(&V) -> bool, D: DeriveAsyncWait>(
        &self,
        key: &Q,
        condition: &mut F,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let (child, metadata) = self.children.min_greater_equal(key);
            if let Some((_, child)) = child {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let result = child.remove_if(key, condition);
                        if result == RemoveResult::Frozen {
                            // When a `Leaf` is frozen, its entries may be being copied to new
                            // `Leaves`.
                            self.wait(async_wait, barrier);
                            return Err(false);
                        } else if result == RemoveResult::Retired {
                            return Ok(self.coalesce(barrier));
                        }
                        return Ok(result);
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                let result = unbounded.remove_if(key, condition);
                if result == RemoveResult::Frozen {
                    self.wait(async_wait, barrier);
                    return Err(false);
                } else if result == RemoveResult::Retired {
                    return Ok(self.coalesce(barrier));
                }
                return Ok(result);
            }
            return Ok(RemoveResult::Fail);
        }
    }

    /// Splits itself into the given leaf nodes, and returns the middle key value.
    pub(super) fn split_leaf_node(
        &self,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        barrier: &Barrier,
    ) -> Option<K> {
        let mut middle_key = None;
        let new_leaves = self.latch.load(Relaxed, barrier).as_ref().unwrap();

        low_key_leaf_node.latch.swap((None, LOCKED), Relaxed);
        high_key_leaf_node.latch.swap((None, LOCKED), Relaxed);

        // It is safe to keep the pointers to the new leaf nodes in this full leaf node since the
        // whole split operation is protected under a single `ebr::Barrier`, and the pointers are
        // only dereferenced during the operation.
        new_leaves
            .low_key_leaf_node
            .swap(addr_of!(*low_key_leaf_node) as *mut _, Relaxed);
        new_leaves
            .high_key_leaf_node
            .swap(addr_of!(*high_key_leaf_node) as *mut _, Relaxed);

        // Builds a list of valid leaves
        #[allow(clippy::type_complexity)]
        let mut entry_array: [Option<(Option<&K>, AtomicArc<Leaf<K, V>>)>;
            DIMENSION.num_entries + 2] = Default::default();
        let mut num_entries = 0;
        let low_key_leaf_ref = new_leaves
            .low_key_leaf
            .load(Relaxed, barrier)
            .as_ref()
            .unwrap();
        let middle_key_ref = low_key_leaf_ref.max().unwrap().0;
        for entry in Scanner::new(&self.children) {
            if new_leaves
                .origin_leaf_key
                .as_ref()
                .map_or_else(|| false, |key| entry.0.borrow() == key)
            {
                entry_array[num_entries].replace((
                    Some(middle_key_ref),
                    new_leaves.low_key_leaf.clone(Relaxed, barrier),
                ));
                num_entries += 1;
                if !new_leaves.high_key_leaf.load(Relaxed, barrier).is_null() {
                    entry_array[num_entries].replace((
                        Some(entry.0),
                        new_leaves.high_key_leaf.clone(Relaxed, barrier),
                    ));
                    num_entries += 1;
                }
            } else {
                entry_array[num_entries].replace((Some(entry.0), entry.1.clone(Relaxed, barrier)));
                num_entries += 1;
            }
        }
        #[allow(clippy::branches_sharing_code)]
        if new_leaves.origin_leaf_key.is_some() {
            // If the origin is a bounded node, assign the unbounded leaf as the high key node's
            // unbounded.
            entry_array[num_entries].replace((None, self.unbounded_child.clone(Relaxed, barrier)));
            num_entries += 1;
        } else {
            // If the origin is an unbounded node, assign the high key node to the high key node's
            // unbounded.
            entry_array[num_entries].replace((
                Some(middle_key_ref),
                new_leaves.low_key_leaf.clone(Relaxed, barrier),
            ));
            num_entries += 1;
            if !new_leaves.high_key_leaf.load(Relaxed, barrier).is_null() {
                entry_array[num_entries]
                    .replace((None, new_leaves.high_key_leaf.clone(Relaxed, barrier)));
                num_entries += 1;
            }
        }
        debug_assert!(num_entries >= 2);

        let low_key_leaf_array_size = num_entries / 2;
        for (i, entry) in entry_array.iter().enumerate() {
            if let Some((k, v)) = entry {
                match (i + 1).cmp(&low_key_leaf_array_size) {
                    Less => {
                        low_key_leaf_node.children.insert_unchecked(
                            k.unwrap().clone(),
                            v.clone(Relaxed, barrier),
                            i,
                        );
                    }
                    Equal => {
                        middle_key.replace(k.unwrap().clone());
                        low_key_leaf_node
                            .unbounded_child
                            .swap((v.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                    }
                    Greater => {
                        if let Some(k) = k.cloned() {
                            high_key_leaf_node.children.insert_unchecked(
                                k,
                                v.clone(Relaxed, barrier),
                                i - low_key_leaf_array_size,
                            );
                        } else {
                            high_key_leaf_node
                                .unbounded_child
                                .swap((v.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                        }
                    }
                }
            } else {
                break;
            }
        }

        debug_assert!(middle_key.is_some());
        middle_key
    }

    /// Commits an on-going structural change.
    #[inline]
    pub(super) fn commit(&self, barrier: &Barrier) {
        // Unfreeze both leaves.
        if let Some(change) = self.latch.load(Relaxed, barrier).as_ref() {
            if let Some(origin_leaf) = change.origin_leaf.swap((None, Tag::None), Relaxed).0 {
                // Make the origin leaf unreachable before making the new leaves updatable.
                origin_leaf.delete_self(Relaxed);
                let _ = origin_leaf.release(barrier);
            }
            let low_key_leaf = change.low_key_leaf.load(Relaxed, barrier).as_ref().unwrap();
            let high_key_leaf = change
                .high_key_leaf
                .load(Relaxed, barrier)
                .as_ref()
                .unwrap();
            let unfrozen = low_key_leaf.thaw();
            debug_assert!(unfrozen);
            let unfrozen = high_key_leaf.thaw();
            debug_assert!(unfrozen);

            // It is safe to dereference those pointers since the whole split operation is under a
            // single `ebr::Barrier`.
            if let Some(low_key_leaf_node) =
                unsafe { change.low_key_leaf_node.load(Relaxed).as_ref() }
            {
                let locked = low_key_leaf_node.latch.swap((None, Tag::None), Relaxed).1;
                debug_assert_eq!(locked, LOCKED);
                low_key_leaf_node.wait_queue.signal();
            }

            if let Some(high_key_leaf_node) =
                unsafe { change.high_key_leaf_node.load(Relaxed).as_ref() }
            {
                let locked = high_key_leaf_node.latch.swap((None, Tag::None), Relaxed).1;
                debug_assert_eq!(locked, LOCKED);
                high_key_leaf_node.wait_queue.signal();
            }
        }

        // Mark the leaf node retired to prevent further locking attempts.
        let (change, _) = self.latch.swap((None, RETIRED), Release);
        self.wait_queue.signal();
        if let Some(change) = change {
            let _ = change.release(barrier);
        }
    }

    /// Rolls back the ongoing split operation.
    #[inline]
    pub(super) fn rollback(&self, barrier: &Barrier) {
        if let Some(change) = self.latch.load(Relaxed, barrier).as_ref() {
            let low_key_leaf = change.low_key_leaf.load(Relaxed, barrier).as_ref().unwrap();
            let high_key_leaf = change
                .high_key_leaf
                .load(Relaxed, barrier)
                .as_ref()
                .unwrap();

            // Roll back the linked list state.
            //
            // `high_key_leaf` must be deleted first in order for `Scanners` not to omit entries.
            let deleted = high_key_leaf.delete_self(Relaxed);
            debug_assert!(deleted);
            let deleted = low_key_leaf.delete_self(Relaxed);
            debug_assert!(deleted);

            if let Some(origin_leaf) = change.origin_leaf.swap((None, Tag::None), Relaxed).0 {
                // Unfreeze the origin.
                let unfrozen = origin_leaf.thaw();
                debug_assert!(unfrozen);

                // Remove the mark from the full leaf node.
                let unmarked = origin_leaf.unmark(Release);
                debug_assert!(unmarked);
            }
        }

        // Unlock the leaf node.
        let (change, _) = self.latch.swap((None, Tag::None), Release);
        self.wait_queue.signal();
        if let Some(change) = change {
            let _ = change.release(barrier);
        }
    }

    /// Cleans up logically deleted [`LeafNode`] instances in the linked list.
    ///
    /// If the target leaf node does not exist in the sub-tree, returns `false`.
    #[inline]
    pub(super) fn cleanup_link<'b, Q>(
        &self,
        key: &Q,
        tranverse_max: bool,
        barrier: &'b Barrier,
    ) -> bool
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let scanner = if tranverse_max {
            if let Some(unbounded) = self.unbounded_child.load(Acquire, barrier).as_ref() {
                Scanner::new(unbounded)
            } else {
                return false;
            }
        } else if let Some(leaf_scanner) = Scanner::max_less(&self.children, key) {
            if let Some((_, child)) = leaf_scanner.get() {
                if let Some(child) = child.load(Acquire, barrier).as_ref() {
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
        scanner.jump(None, barrier).map(|s| s.jump(None, barrier));
        true
    }

    /// Waits for the lock on the [`LeafNode`] to be released.
    #[inline]
    pub(super) fn wait<D: DeriveAsyncWait>(&self, async_wait: &mut D, barrier: &Barrier) {
        let waiter = || {
            let ptr = self.latch.load(Relaxed, barrier);
            if !ptr.is_null() || ptr.tag() == LOCKED {
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
    /// Returns an error if retry is required.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn split_leaf<D: DeriveAsyncWait>(
        &self,
        key: K,
        value: V,
        full_leaf_key: Option<&K>,
        full_leaf_ptr: Ptr<Leaf<K, V>>,
        full_leaf: &AtomicArc<Leaf<K, V>>,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        let new_leaves = if let Ok((_, ptr)) = self.latch.compare_exchange(
            Ptr::null(),
            (
                Some(Arc::new(StructuralChange {
                    origin_leaf_key: None,
                    origin_leaf: full_leaf.clone(Relaxed, barrier),
                    low_key_leaf: AtomicArc::null(),
                    high_key_leaf: AtomicArc::null(),
                    low_key_leaf_node: AtomicPtr::default(),
                    high_key_leaf_node: AtomicPtr::default(),
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
            barrier,
        ) {
            ptr.as_ref().unwrap()
        } else {
            self.wait(async_wait, barrier);
            return Err((key, value));
        };

        if self.retired(Relaxed) {
            let (change, _) = self.latch.swap((None, Tag::None), Relaxed);
            self.wait_queue.signal();
            drop(change);
            return Ok(InsertResult::Retired(key, value));
        }
        if full_leaf_ptr != full_leaf.load(Relaxed, barrier) {
            let (change, _) = self.latch.swap((None, Tag::None), Relaxed);
            self.wait_queue.signal();
            drop(change);
            return Err((key, value));
        }

        if let Some(full_leaf_key) = full_leaf_key {
            let ptr = addr_of!(new_leaves.origin_leaf_key) as *mut Option<K>;
            unsafe {
                ptr.write(Some(full_leaf_key.clone()));
            }
        }

        let target = full_leaf_ptr.as_ref().unwrap();
        let mut low_key_leaf_arc = None;
        let mut high_key_leaf_arc = None;

        // Distribute entries to two leaves after make the target retired.
        let result = target.freeze_and_distribute(&mut low_key_leaf_arc, &mut high_key_leaf_arc);
        debug_assert!(result);

        if let Some(low_key_leaf) = low_key_leaf_arc.take() {
            new_leaves
                .low_key_leaf
                .swap((Some(low_key_leaf), Tag::None), Relaxed);
            if let Some(high_key_leaf) = high_key_leaf_arc.take() {
                new_leaves
                    .high_key_leaf
                    .swap((Some(high_key_leaf), Tag::None), Relaxed);
            }
        } else {
            // No valid keys in the full leaf.
            new_leaves
                .low_key_leaf
                .swap((Some(Arc::new(Leaf::new())), Tag::None), Relaxed);
        }

        // When a new leaf is added to the linked list, the leaf is marked to let `Scanners`
        // acknowledge that the newly added leaf may contain keys that are smaller than those
        // having been `scanned`.
        let low_key_leaf_ptr = new_leaves.low_key_leaf.load(Relaxed, barrier);
        let high_key_leaf_ptr = new_leaves.high_key_leaf.load(Relaxed, barrier);
        let unused_leaf = if let Some(high_key_leaf) = high_key_leaf_ptr.as_ref() {
            // From here, `Scanners` can reach the new leaves.
            let result =
                target.push_back(high_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());
            let result =
                target.push_back(low_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());

            // Take the max key value stored in the low key leaf as the leaf key.
            let low_key_leaf = low_key_leaf_ptr.as_ref().unwrap();
            let max_key = low_key_leaf.max().unwrap().0;

            // Need to freeze the leaf before trying to make it reachable.
            let frozen = low_key_leaf.freeze();
            debug_assert!(frozen);

            match self.children.insert(
                max_key.clone(),
                new_leaves.low_key_leaf.clone(Relaxed, barrier),
            ) {
                InsertResult::Success => (),
                InsertResult::Duplicate(..)
                | InsertResult::Frozen(..)
                | InsertResult::Retry(..) => unreachable!(),
                InsertResult::Full(_, _) | InsertResult::Retired(_, _) => {
                    // Need to freeze the other leaf.
                    let frozen = high_key_leaf.freeze();
                    debug_assert!(frozen);
                    return Ok(InsertResult::Full(key, value));
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
            let high_key_leaf = new_leaves.high_key_leaf.swap((None, Tag::None), Relaxed).0;
            full_leaf.swap((high_key_leaf, Tag::None), Release).0
        } else {
            // From here, `Scanners` can reach the new leaf.
            //
            // The full leaf is marked so that readers know that the next leaves may contain
            // smaller keys.
            let result =
                target.push_back(low_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());

            // Mark the full leaf deleted before making the new one reachable and updatable.
            let deleted = target.delete_self(Release);
            debug_assert!(deleted);

            full_leaf
                .swap(
                    new_leaves.low_key_leaf.swap((None, Tag::None), Release),
                    Release,
                )
                .0
        };

        // Unlocks the leaf node.
        let (change, _) = self.latch.swap((None, Tag::None), Release);
        self.wait_queue.signal();
        if let Some(change) = change {
            let _ = change.release(barrier);
        }

        if let Some(unused_leaf) = unused_leaf {
            let _ = unused_leaf.release(barrier);
        }

        // Since a new leaf has been inserted, the caller can retry.
        Ok(InsertResult::Retry(key, value))
    }

    /// Tries to coalesce empty or obsolete leaves after a successful removal of an entry.
    fn coalesce<Q>(&self, barrier: &Barrier) -> RemoveResult
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut uncleaned_leaf = false;
        let mut prev_valid_leaf = None;
        while let Some(lock) = Locker::try_lock(self, barrier) {
            prev_valid_leaf.take();
            for entry in Scanner::new(&self.children) {
                let leaf_ptr = entry.1.load(Relaxed, barrier);
                let leaf = leaf_ptr.as_ref().unwrap();
                if leaf.retired() {
                    let deleted = leaf.delete_self(Relaxed);
                    debug_assert!(deleted);
                    let result = self.children.remove_if(entry.0.borrow(), &mut |_| true);
                    debug_assert_ne!(result, RemoveResult::Fail);

                    // The pointer is nullified after the metadata of `self.children` is updated so
                    // that readers are able to retry when they find it being `null`.
                    if let Some(leaf) = entry.1.swap((None, Tag::None), Release).0 {
                        let _ = leaf.release(barrier);
                        if let Some(prev_leaf) = prev_valid_leaf.as_ref() {
                            // One jump is sufficient.
                            Scanner::new(*prev_leaf).jump(None, barrier);
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
                let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
                if let Some(unbounded) = unbounded_ptr.as_ref() {
                    if unbounded.retired() {
                        let deleted = unbounded.delete_self(Relaxed);
                        debug_assert!(deleted);

                        // It has to mark the pointer in order to prevent `LeafNode::insert` from
                        // replacing it with a new `Leaf`.
                        if let Some(obsolete_leaf) =
                            self.unbounded_child.swap((None, RETIRED), Release).0
                        {
                            let _ = obsolete_leaf.release(barrier);
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
            if !self.has_retired_leaf(barrier) {
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
    fn has_retired_leaf(&self, barrier: &Barrier) -> bool {
        let mut has_valid_leaf = false;
        for entry in Scanner::new(&self.children) {
            let leaf_ptr = entry.1.load(Relaxed, barrier);
            if let Some(leaf) = leaf_ptr.as_ref() {
                if leaf.retired() {
                    return true;
                }
                has_valid_leaf = true;
            }
        }
        if !has_valid_leaf {
            let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.retired() {
                    return true;
                }
            }
        }
        false
    }
}

/// [`Locker`] holds exclusive access to a [`Leaf`].
pub struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    leaf_node: &'n LeafNode<K, V>,
}

impl<'n, K, V> Locker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Acquires exclusive lock on the [`LeafNode`].
    #[inline]
    pub(super) fn try_lock(
        leaf_node: &'n LeafNode<K, V>,
        barrier: &'n Barrier,
    ) -> Option<Locker<'n, K, V>> {
        if leaf_node
            .latch
            .compare_exchange(Ptr::null(), (None, LOCKED), Acquire, Relaxed, barrier)
            .is_ok()
        {
            Some(Locker { leaf_node })
        } else {
            None
        }
    }
}

impl<'n, K, V> Drop for Locker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    #[inline]
    fn drop(&mut self) {
        debug_assert_eq!(self.leaf_node.latch.tag(Relaxed), LOCKED);
        self.leaf_node.latch.swap((None, Tag::None), Release);
        self.leaf_node.wait_queue.signal();
    }
}

/// [`StructuralChange`] stores intermediate results during a split/merge operation.
pub struct StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    origin_leaf_key: Option<K>,
    origin_leaf: AtomicArc<Leaf<K, V>>,
    low_key_leaf: AtomicArc<Leaf<K, V>>,
    high_key_leaf: AtomicArc<Leaf<K, V>>,
    low_key_leaf_node: AtomicPtr<LeafNode<K, V>>,
    high_key_leaf_node: AtomicPtr<LeafNode<K, V>>,
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use tokio::sync;

    #[test]
    fn basic() {
        let barrier = Barrier::new();
        let leaf_node: LeafNode<String, String> = LeafNode::new();
        assert!(matches!(
            leaf_node.insert(
                "MY GOODNESS!".to_owned(),
                "OH MY GOD!!".to_owned(),
                &mut (),
                &barrier
            ),
            Ok(InsertResult::Success)
        ));
        assert!(matches!(
            leaf_node.insert(
                "GOOD DAY".to_owned(),
                "OH MY GOD!!".to_owned(),
                &mut (),
                &barrier
            ),
            Ok(InsertResult::Success)
        ));
        assert_eq!(
            leaf_node.search("MY GOODNESS!", &barrier).unwrap(),
            "OH MY GOD!!"
        );
        assert_eq!(
            leaf_node.search("GOOD DAY", &barrier).unwrap(),
            "OH MY GOD!!"
        );
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("GOOD DAY", &mut |v| v == "OH MY", &mut (), &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>(
                "GOOD DAY",
                &mut |v| v == "OH MY GOD!!",
                &mut (),
                &barrier
            ),
            Ok(RemoveResult::Success)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("GOOD", &mut |v| v == "OH MY", &mut (), &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, _>("MY GOODNESS!", &mut |_| true, &mut (), &barrier),
            Ok(RemoveResult::Retired)
        ));
        assert!(matches!(
            leaf_node.insert("HI".to_owned(), "HO".to_owned(), &mut (), &barrier),
            Ok(InsertResult::Retired(..))
        ));
    }

    #[test]
    fn bulk() {
        let barrier = Barrier::new();
        let leaf_node: LeafNode<usize, usize> = LeafNode::new();
        for k in 0..1024 {
            let mut result = leaf_node.insert(k, k, &mut (), &barrier);
            if result.is_err() {
                result = leaf_node.insert(k, k, &mut (), &barrier);
            }
            match result.unwrap() {
                InsertResult::Success => {
                    assert_eq!(leaf_node.search(&k, &barrier), Some(&k));
                    continue;
                }
                InsertResult::Duplicate(..)
                | InsertResult::Frozen(..)
                | InsertResult::Retired(..) => unreachable!(),
                InsertResult::Full(_, _) => {
                    leaf_node.rollback(&barrier);
                    for r in 0..(k - 1) {
                        assert_eq!(leaf_node.search(&r, &barrier), Some(&r));
                        assert!(leaf_node
                            .remove_if::<_, _, _>(&r, &mut |_| true, &mut (), &barrier)
                            .is_ok());
                        assert_eq!(leaf_node.search(&r, &barrier), None);
                    }
                    assert_eq!(leaf_node.search(&(k - 1), &barrier), Some(&(k - 1)));
                    assert_eq!(
                        leaf_node.remove_if::<_, _, _>(&(k - 1), &mut |_| true, &mut (), &barrier),
                        Ok(RemoveResult::Retired)
                    );
                    assert_eq!(leaf_node.search(&(k - 1), &barrier), None);
                    break;
                }
                InsertResult::Retry(..) => {
                    assert!(leaf_node.insert(k, k, &mut (), &barrier).is_ok());
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel() {
        let num_tasks = 8;
        let workload_size = 64;
        let barrier = Arc::new(sync::Barrier::new(num_tasks));
        for _ in 0..16 {
            let leaf_node = Arc::new(LeafNode::new());
            assert!(leaf_node
                .insert(usize::MAX, usize::MAX, &mut (), &Barrier::new())
                .is_ok());
            let mut task_handles = Vec::with_capacity(num_tasks);
            for task_id in 0..num_tasks {
                let barrier_cloned = barrier.clone();
                let leaf_node_cloned = leaf_node.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_cloned.wait().await;
                    let barrier = Barrier::new();
                    let mut max_key = None;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        loop {
                            if let Ok(r) = leaf_node_cloned.insert(id, id, &mut (), &barrier) {
                                match r {
                                    InsertResult::Success => {
                                        match leaf_node_cloned.insert(id, id, &mut (), &barrier) {
                                            Ok(InsertResult::Duplicate(..)) | Err(_) => (),
                                            _ => unreachable!(),
                                        }
                                        break;
                                    }
                                    InsertResult::Full(..) => {
                                        leaf_node_cloned.rollback(&barrier);
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
                        assert_eq!(leaf_node_cloned.search(&id, &barrier), Some(&id));
                    }
                    for id in range {
                        if max_key.map_or(false, |m| m == id) {
                            break;
                        }
                        let mut removed = false;
                        loop {
                            match leaf_node_cloned.remove_if::<_, _, _>(
                                &id,
                                &mut |_| true,
                                &mut (),
                                &barrier,
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
                        assert!(leaf_node_cloned.search(&id, &barrier).is_none(), "{}", id);
                        if let Ok(RemoveResult::Success) = leaf_node_cloned.remove_if::<_, _, _>(
                            &id,
                            &mut |_| true,
                            &mut (),
                            &barrier,
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
                .remove_if::<_, _, _>(&usize::MAX, &mut |_| true, &mut (), &Barrier::new())
                .is_ok());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 16_usize;
        let workload_size = 64_usize;
        for _ in 0..16 {
            for k in 0..=workload_size {
                let barrier = Arc::new(sync::Barrier::new(num_tasks));
                let leaf_node: Arc<LeafNode<usize, usize>> = Arc::new(LeafNode::new());
                let inserted: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
                let mut task_handles = Vec::with_capacity(num_tasks);
                for _ in 0..num_tasks {
                    let barrier_clone = barrier.clone();
                    let leaf_node_clone = leaf_node.clone();
                    let inserted_clone = inserted.clone();
                    task_handles.push(tokio::spawn(async move {
                        {
                            barrier_clone.wait().await;
                            let barrier = Barrier::new();
                            match leaf_node_clone.insert(k, k, &mut (), &barrier) {
                                Ok(InsertResult::Success) => {
                                    assert!(!inserted_clone.swap(true, Relaxed));
                                }
                                Ok(InsertResult::Full(_, _) | InsertResult::Retired(_, _)) => {
                                    leaf_node_clone.rollback(&barrier);
                                }
                                _ => (),
                            };
                        }
                        {
                            barrier_clone.wait().await;
                            let barrier = Barrier::new();
                            for i in 0..workload_size {
                                if i != k {
                                    if let Ok(
                                        InsertResult::Full(_, _) | InsertResult::Retired(_, _),
                                    ) = leaf_node_clone.insert(i, i, &mut (), &barrier)
                                    {
                                        leaf_node_clone.rollback(&barrier);
                                    }
                                }
                                assert_eq!(leaf_node_clone.search(&k, &barrier).unwrap(), &k);
                            }
                            for i in 0..workload_size {
                                let max_scanner =
                                    leaf_node_clone.max_le_appr(&k, &barrier).unwrap();
                                assert!(*max_scanner.get().unwrap().0 <= k);
                                let mut min_scanner = leaf_node_clone.min(&barrier).unwrap();
                                if let Some((k_ref, v_ref)) = min_scanner.next() {
                                    assert_eq!(*k_ref, *v_ref);
                                    assert!(*k_ref <= k);
                                } else {
                                    let (k_ref, v_ref) =
                                        min_scanner.jump(None, &barrier).unwrap().get().unwrap();
                                    assert_eq!(*k_ref, *v_ref);
                                    assert!(*k_ref <= k);
                                }
                                let _result = leaf_node_clone.remove_if::<_, _, _>(
                                    &i,
                                    &mut |v| *v != k,
                                    &mut (),
                                    &barrier,
                                );
                                assert_eq!(leaf_node_clone.search(&k, &barrier).unwrap(), &k);
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
