use super::leaf::{InsertResult, RemoveResult, Scanner, DIMENSION};
use super::Leaf;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::WaitQueue;
use crate::LinkedList;

use std::borrow::Borrow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::ptr::addr_of;
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
    pub fn new() -> LeafNode<K, V> {
        LeafNode {
            children: Leaf::new(),
            unbounded_child: AtomicArc::null(),
            latch: AtomicArc::null(),
            wait_queue: WaitQueue::default(),
        }
    }

    /// Returns `true` if the [`LeafNode`] has retired.
    pub fn retired(&self, mo: Ordering) -> bool {
        self.unbounded_child.tag(mo) == RETIRED
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<&'b V>
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
    pub fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
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
    pub fn max_le_appr<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>>
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
    pub fn insert<const ASYNC: bool>(
        &self,
        key: K,
        value: V,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        loop {
            let (child, metadata) = self.children.min_greater_equal(&key);
            if let Some((child_key, child)) = child {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child_ref) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        match child_ref.insert(key, value) {
                            InsertResult::Success => return Ok(InsertResult::Success),
                            InsertResult::Duplicate(key, value) => {
                                return Ok(InsertResult::Duplicate(key, value));
                            }
                            InsertResult::Full(key, value) | InsertResult::Retired(key, value) => {
                                return self.split_leaf::<ASYNC>(
                                    key,
                                    value,
                                    Some(child_key),
                                    child_ptr,
                                    child,
                                    barrier,
                                );
                            }
                            InsertResult::Frozen(key, value) => {
                                // The `Leaf` is being split: retry.
                                self.wait::<ASYNC>(true, barrier);
                                return Err((key, value));
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
                match unbounded.insert(key, value) {
                    InsertResult::Success => return Ok(InsertResult::Success),
                    InsertResult::Duplicate(key, value) => {
                        return Ok(InsertResult::Duplicate(key, value));
                    }
                    InsertResult::Full(key, value) | InsertResult::Retired(key, value) => {
                        return self.split_leaf::<ASYNC>(
                            key,
                            value,
                            None,
                            unbounded_ptr,
                            &self.unbounded_child,
                            barrier,
                        );
                    }
                    InsertResult::Frozen(key, value) => {
                        self.wait::<ASYNC>(true, barrier);
                        return Err((key, value));
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
    pub fn remove_if<Q, F: FnMut(&V) -> bool, const ASYNC: bool>(
        &self,
        key: &Q,
        condition: &mut F,
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
                        if child.frozen() {
                            // When a `Leaf` is frozen, its entries may be being copied to new
                            // `Leaves`.
                            self.wait::<ASYNC>(true, barrier);
                            return Err(result != RemoveResult::Fail);
                        }
                        if result == RemoveResult::Retired {
                            return Ok(self.coalesce(key, barrier));
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
                if unbounded.frozen() {
                    self.wait::<ASYNC>(true, barrier);
                    return Err(result != RemoveResult::Fail);
                }
                if result == RemoveResult::Retired {
                    return Ok(self.coalesce(key, barrier));
                }
                return Ok(result);
            }
            return Ok(RemoveResult::Fail);
        }
    }

    /// Splits a full leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if retry is required.
    #[allow(clippy::too_many_lines)]
    fn split_leaf<const ASYNC: bool>(
        &self,
        key: K,
        value: V,
        full_leaf_key: Option<&K>,
        full_leaf_ptr: Ptr<Leaf<K, V>>,
        full_leaf: &AtomicArc<Leaf<K, V>>,
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
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
            barrier,
        ) {
            ptr.as_ref().unwrap()
        } else {
            self.wait::<ASYNC>(false, barrier);
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
        if !result {
            let (change, _) = self.latch.swap((None, Tag::None), Relaxed);
            self.wait_queue.signal();
            drop(change);
            return Err((key, value));
        } // TODO: assert!(result);

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

            // Takes the max key value stored in the low key leaf as the leaf key.
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
                InsertResult::Duplicate(..) | InsertResult::Frozen(..) => unreachable!(),
                InsertResult::Full(_, _) | InsertResult::Retired(_, _) => {
                    // Need to freeze the other leaf.
                    let frozen = high_key_leaf.freeze();
                    debug_assert!(frozen);
                    return Ok(InsertResult::Full(key, value));
                }
            };

            // Mark the full leaf deleted before making the new one reachable and updatable.
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
            //
            // There is a possibility that entries were removed from the replaced leaf node whereas
            // those entries still remain in `unused_leaf`; if that happens, iterators may see
            // the removed entries momentarily.
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
            barrier.reclaim(change);
        }

        if let Some(unused_leaf) = unused_leaf {
            barrier.reclaim(unused_leaf);
        }

        // Traverse several leaf nodes in order to cleanup deprecated links.
        self.max_le_appr(key.borrow(), barrier);

        // Since a new leaf has been inserted, the caller can retry.
        Err((key, value))
    }

    /// Splits itself into the given leaf nodes, and returns the middle key value.
    pub fn split_leaf_node(
        &self,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        barrier: &Barrier,
    ) -> Option<K> {
        let mut middle_key = None;

        debug_assert!(!self.latch.load(Relaxed, barrier).is_null());
        let new_leaves_ref = self.latch.load(Relaxed, barrier).as_ref().unwrap();

        // Builds a list of valid leaves
        #[allow(clippy::type_complexity)]
        let mut entry_array: [Option<(Option<&K>, AtomicArc<Leaf<K, V>>)>;
            DIMENSION.num_entries + 2] = Default::default();
        let mut num_entries = 0;
        let low_key_leaf_ref = new_leaves_ref
            .low_key_leaf
            .load(Relaxed, barrier)
            .as_ref()
            .unwrap();
        let middle_key_ref = low_key_leaf_ref.max().unwrap().0;
        for entry in Scanner::new(&self.children) {
            if new_leaves_ref
                .origin_leaf_key
                .as_ref()
                .map_or_else(|| false, |key| entry.0.borrow() == key)
            {
                entry_array[num_entries].replace((
                    Some(middle_key_ref),
                    new_leaves_ref.low_key_leaf.clone(Relaxed, barrier),
                ));
                num_entries += 1;
                if !new_leaves_ref
                    .high_key_leaf
                    .load(Relaxed, barrier)
                    .is_null()
                {
                    entry_array[num_entries].replace((
                        Some(entry.0),
                        new_leaves_ref.high_key_leaf.clone(Relaxed, barrier),
                    ));
                    num_entries += 1;
                }
            } else {
                entry_array[num_entries].replace((Some(entry.0), entry.1.clone(Relaxed, barrier)));
                num_entries += 1;
            }
        }
        #[allow(clippy::branches_sharing_code)]
        if new_leaves_ref.origin_leaf_key.is_some() {
            // If the origin is a bounded node, assign the unbounded leaf as the high key node's
            // unbounded.
            entry_array[num_entries].replace((None, self.unbounded_child.clone(Relaxed, barrier)));
            num_entries += 1;
        } else {
            // If the origin is an unbounded node, assign the high key node to the high key node's
            // unbounded.
            entry_array[num_entries].replace((
                Some(middle_key_ref),
                new_leaves_ref.low_key_leaf.clone(Relaxed, barrier),
            ));
            num_entries += 1;
            if !new_leaves_ref
                .high_key_leaf
                .load(Relaxed, barrier)
                .is_null()
            {
                entry_array[num_entries]
                    .replace((None, new_leaves_ref.high_key_leaf.clone(Relaxed, barrier)));
                num_entries += 1;
            }
        }
        debug_assert!(num_entries >= 2);

        let low_key_leaf_array_size = num_entries / 2;
        for (index, entry) in entry_array.iter().enumerate() {
            if let Some(entry) = entry {
                match (index + 1).cmp(&low_key_leaf_array_size) {
                    Less => {
                        low_key_leaf_node
                            .children
                            .insert(entry.0.unwrap().clone(), entry.1.clone(Relaxed, barrier));
                    }
                    Equal => {
                        middle_key.replace(entry.0.unwrap().clone());
                        low_key_leaf_node
                            .unbounded_child
                            .swap((entry.1.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                    }
                    Greater => {
                        if let Some(key) = entry.0 {
                            high_key_leaf_node
                                .children
                                .insert(key.clone(), entry.1.clone(Relaxed, barrier));
                        } else {
                            high_key_leaf_node
                                .unbounded_child
                                .swap((entry.1.get_arc(Relaxed, barrier), Tag::None), Relaxed);
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
    pub fn commit(&self, barrier: &Barrier) {
        // Unfreeze both leaves.
        if let Some(change) = self.latch.load(Relaxed, barrier).as_ref() {
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
        }

        // Mark the leaf node retired to prevent further locking attempts.
        let (change, _) = self.latch.swap((None, RETIRED), Release);
        self.wait_queue.signal();
        if let Some(change) = change {
            if let Some(obsolete_leaf) = change.origin_leaf.swap((None, Tag::None), Relaxed).0 {
                // Makes the leaf unreachable before dropping it.
                obsolete_leaf.delete_self(Relaxed);
                barrier.reclaim(obsolete_leaf);
            }
        }
    }

    /// Rolls back the ongoing split operation.
    pub fn rollback(&self, barrier: &Barrier) {
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

            if let Some(origin) = change.origin_leaf.swap((None, Tag::None), Relaxed).0 {
                // Unfreeze the origin.
                let unfrozen = origin.thaw();
                debug_assert!(unfrozen);

                // Remove the mark from the full leaf node.
                //
                // This unmarking has to be a release-store, otherwise it can be re-ordered
                // before previous `delete_self` calls.
                let unmarked = origin.unmark(Release);
                debug_assert!(unmarked);
            }
        }

        // Unlock the leaf node.
        let (change, _) = self.latch.swap((None, Tag::None), Release);
        self.wait_queue.signal();
        if let Some(change) = change {
            barrier.reclaim(change);
        }
    }

    /// Waits for the lock on the [`LeafNode`] to be released.
    pub(super) fn wait<const ASYNC: bool>(&self, expect_null: bool, barrier: &Barrier) {
        if ASYNC {
            // Currently, asynchronous waiting is not implemented.
            return;
        }
        let _result = self.wait_queue.wait(|| {
            let ptr = self.latch.load(Relaxed, barrier);
            let is_null = ptr.is_null();
            if expect_null && is_null {
                // The `LeafNode` may be locked, but it does not care.
                return Ok(());
            }
            if !is_null || ptr.tag() == LOCKED {
                // The `LeafNode` is being split or locked.
                return Err(());
            }
            Ok(())
        });
    }

    /// Tries to coalesce empty or obsolete leaves after a successful removal of an entry.
    fn coalesce<Q>(&self, removed_key: &Q, barrier: &Barrier) -> RemoveResult
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        while let Some(lock) = Locker::try_lock(self, barrier) {
            let mut has_valid_leaf = false;
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
                        barrier.reclaim(leaf);
                    }
                } else {
                    has_valid_leaf = true;
                }
            }

            // The unbounded leaf becomes unreachable after all the other leaves are gone.
            let fully_empty = if has_valid_leaf {
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
                            barrier.reclaim(obsolete_leaf);
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
                // Traverse several leaf nodes in order to cleanup deprecated links.
                self.max_le_appr(removed_key, barrier);
                return RemoveResult::Success;
            }
        }

        // Locking failed: give up expecting that the lock owner eventually cleans up the leaf.
        RemoveResult::Success
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
    pub fn try_lock(
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
            leaf_node.insert::<false>(
                "MY GOODNESS!".to_owned(),
                "OH MY GOD!!".to_owned(),
                &barrier
            ),
            Ok(InsertResult::Success)
        ));
        assert!(matches!(
            leaf_node.insert::<false>("GOOD DAY".to_owned(), "OH MY GOD!!".to_owned(), &barrier),
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
            leaf_node.remove_if::<_, _, false>("GOOD DAY", &mut |v| v == "OH MY", &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, false>("GOOD DAY", &mut |v| v == "OH MY GOD!!", &barrier),
            Ok(RemoveResult::Success)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, false>("GOOD", &mut |v| v == "OH MY", &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if::<_, _, false>("MY GOODNESS!", &mut |_| true, &barrier),
            Ok(RemoveResult::Retired)
        ));
        assert!(matches!(
            leaf_node.insert::<false>("HI".to_owned(), "HO".to_owned(), &barrier),
            Ok(InsertResult::Retired(..))
        ));
    }

    #[test]
    fn bulk() {
        let barrier = Barrier::new();
        let leaf_node: LeafNode<usize, usize> = LeafNode::new();
        for k in 0..1024 {
            let mut result = leaf_node.insert::<false>(k, k, &barrier);
            if result.is_err() {
                result = leaf_node.insert::<false>(k, k, &barrier);
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
                        assert_eq!(
                            leaf_node.remove_if::<_, _, false>(&r, &mut |_| true, &barrier),
                            Ok(RemoveResult::Success)
                        );
                        assert_eq!(leaf_node.search(&r, &barrier), None);
                    }
                    assert_eq!(leaf_node.search(&(k - 1), &barrier), Some(&(k - 1)));
                    assert_eq!(
                        leaf_node.remove_if::<_, _, false>(&(k - 1), &mut |_| true, &barrier),
                        Ok(RemoveResult::Retired)
                    );
                    assert_eq!(leaf_node.search(&(k - 1), &barrier), None);
                    break;
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
                .insert::<false>(usize::MAX, usize::MAX, &Barrier::new())
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
                            if let Ok(r) = leaf_node_cloned.insert::<false>(id, id, &barrier) {
                                match r {
                                    InsertResult::Success => {
                                        match leaf_node_cloned.insert::<false>(id, id, &barrier) {
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
                                    InsertResult::Frozen(..) => {
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
                            match leaf_node_cloned.remove_if::<_, _, false>(
                                &id,
                                &mut |_| true,
                                &barrier,
                            ) {
                                Ok(r) => match r {
                                    RemoveResult::Success => break,
                                    RemoveResult::Fail => {
                                        assert!(removed);
                                        break;
                                    }
                                    RemoveResult::Retired => unreachable!(),
                                },
                                Err(r) => removed |= r,
                            }
                        }
                        assert!(leaf_node_cloned.search(&id, &barrier).is_none(), "{}", id);
                        if let Ok(RemoveResult::Success) =
                            leaf_node_cloned.remove_if::<_, _, false>(&id, &mut |_| true, &barrier)
                        {
                            unreachable!()
                        }
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert!(matches!(
                leaf_node.remove_if::<_, _, false>(&usize::MAX, &mut |_| true, &Barrier::new()),
                Ok(RemoveResult::Success | RemoveResult::Retired)
            ));
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
                            match leaf_node_clone.insert::<false>(k, k, &barrier) {
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
                                    ) = leaf_node_clone.insert::<false>(i, i, &barrier)
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
                                let _result = leaf_node_clone.remove_if::<_, _, false>(
                                    &i,
                                    &mut |v| *v != k,
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
