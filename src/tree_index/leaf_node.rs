use super::leaf::{Scanner, ARRAY_SIZE};
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::LinkedList;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// Leaf node.
///
/// The layout of a leaf node: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
pub struct LeafNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Child leaves.
    ///
    /// The pointer to the unbounded leaf storing a non-zero tag indicates that the leaf is obsolete.
    #[allow(clippy::type_complexity)]
    leaves: (Leaf<K, AtomicArc<Leaf<K, V>>>, AtomicArc<Leaf<K, V>>),
    /// New leaves in an intermediate state during merge and split.
    ///
    /// A valid pointer stored in the variable acts as a mutex for merge and split operations.
    new_leaves: AtomicArc<NewLeaves<K, V>>,
}

impl<K, V> LeafNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new empty leaf node.
    pub fn new() -> LeafNode<K, V> {
        LeafNode {
            leaves: (Leaf::new(), AtomicArc::null()),
            new_leaves: AtomicArc::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Checks if the leaf node is obsolete.
    pub fn obsolete(&self, barrier: &Barrier) -> bool {
        if self.leaves.0.obsolete() {
            let unbounded_ptr = self.leaves.1.load(Relaxed, barrier);
            // The unbounded leaf is specially marked when becoming obsolete.
            return unbounded_ptr.tag() == Tag::First;
        }
        false
    }

    /// Searches for an entry associated with the given key.
    pub fn search<'b, Q>(
        &self,
        key: &'b Q,
        barrier: &'b Barrier,
    ) -> Result<Option<&'b V>, SearchError>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.leaves.0).min_greater_equal(key);
            if let Some((_, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata.
                    //  - Writer: start to insert an intermediate low key leaf
                    //  - Reader: read the metadata not including the intermediate low key leaf
                    //  - Writer: insert the intermediate low key leaf and replace the high key leaf pointer
                    //  - Reader: read the new high key leaf pointer
                    // Consequently, the reader may miss keys in the low key leaf.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return Ok(child_ref.search(key));
                }
                // `child_ptr` being null indicates that the leaf node is bound to be freed.
                return Err(SearchError::Retry);
            }
            let unbounded_ptr = (self.leaves.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see above.
                    continue;
                }
                return Ok(unbounded_ref.search(key));
            }
            if unbounded_ptr.tag() == Tag::First {
                // The leaf node has become obsolete.
                return Err(SearchError::Retry);
            }
            // The `TreeIndex` is empty.
            return Err(SearchError::Empty);
        }
    }

    /// Returns the minimum key entry.
    pub fn min<'b>(&self, barrier: &'b Barrier) -> Result<Scanner<'b, K, V>, SearchError> {
        loop {
            let mut scanner = Scanner::new(&self.leaves.0);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return Ok(Scanner::new(child_ref));
                }
                // `child_ptr` being null indicates that the leaf node is bound to be freed.
                return Err(SearchError::Retry);
            }
            let unbounded_ptr = (self.leaves.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return Ok(Scanner::new(unbounded_ref));
            }
            if unbounded_ptr.tag() == Tag::First {
                // The leaf node has become obsolete.
                return Err(SearchError::Retry);
            }
            // The `TreeIndex` is empty.
            return Err(SearchError::Empty);
        }
    }

    /// Returns the maximum key entry less than the given key.
    pub fn max_less<'b>(
        &self,
        key: &K,
        barrier: &'b Barrier,
    ) -> Result<Scanner<'b, K, V>, SearchError> {
        loop {
            let mut scanner = Scanner::max_less(&self.leaves.0, key);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return Ok(Scanner::max_less(child_ref, key));
                }
                // `child_ptr` being null indicates that the leaf node is bound to be freed.
                return Err(SearchError::Retry);
            }
            let unbounded_ptr = (self.leaves.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                return Ok(Scanner::max_less(unbounded_ref, key));
            }
            if unbounded_ptr.tag() == Tag::First {
                // The leaf node has become obsolete.
                return Err(SearchError::Retry);
            }
            // The `TreeIndex` is empty.
            return Err(SearchError::Empty);
        }
    }

    /// Inserts a key-value pair.
    pub fn insert(&self, key: K, value: V, barrier: &Barrier) -> Result<(), InsertError<K, V>> {
        loop {
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return child_ref.insert(key, value).map_or_else(
                        || Ok(()),
                        |result| {
                            if result.1 {
                                return Err(InsertError::Duplicated(result.0));
                            }
                            debug_assert!(child_ref.full());
                            if !self.split_leaf(Some(child_key.clone()), child_ptr, child, barrier)
                            {
                                return Err(InsertError::Full(result.0));
                            }
                            Err(InsertError::Retry(result.0))
                        },
                    );
                }
                // `child_ptr` being null indicates that the leaf node is bound to be freed.
                return Err(InsertError::Retry((key, value)));
            }

            let mut unbounded_ptr = self.leaves.1.load(Acquire, barrier);
            while unbounded_ptr.is_null() {
                if unbounded_ptr.tag() == Tag::First {
                    // The leaf node has become obsolete.
                    break;
                }
                // Tries to allocate a new leaf.
                //  - It only happens when the first entry is being inserted into the
                // `TreeIndex`.
                match self.leaves.1.compare_exchange(
                    unbounded_ptr,
                    (Some(Arc::new(Leaf::new())), Tag::None),
                    AcqRel,
                    Acquire,
                ) {
                    Ok((_, ptr)) => {
                        unbounded_ptr = ptr;
                        break;
                    }
                    Err((_, actual)) => {
                        unbounded_ptr = actual;
                    }
                }
            }
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see 'InternalNode::search'.
                    continue;
                }
                // Tries to insert into the unbounded leaf, and tries to split the unbounded if
                // it is full.
                return unbounded_ref.insert(key, value).map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(InsertError::Duplicated(result.0));
                        }
                        debug_assert!(unbounded_ref.full());
                        if !self.split_leaf(None, unbounded_ptr, &self.leaves.1, barrier) {
                            return Err(InsertError::Full(result.0));
                        }
                        Err(InsertError::Retry(result.0))
                    },
                );
            }

            // unbounded_shared being null indicates that the leaf node is bound to be freed.
            return Err(InsertError::Retry((key, value)));
        }
    }

    /// Removes an entry associated with the given key.
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(
        &self,
        key_ref: &Q,
        condition: &mut F,
        barrier: &Barrier,
    ) -> Result<bool, RemoveError>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let result = (self.leaves.0).min_greater_equal(key_ref);
            if let Some((_, child)) = result.0 {
                let child_ptr = child.load(Acquire, barrier);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    let (removed, full, empty) = child_ref.remove_if(key_ref, condition);
                    if full && !self.check_full_leaf(key_ref, child_ptr, barrier) {
                        // Data race resolution.
                        //  - Insert: start to insert into a full leaf
                        //  - Remove: start removing an entry from the leaf after pointer validation
                        //  - Insert: find the leaf full, thus splitting and update
                        //  - Remove: find the leaf full, and the leaf node is not locked, returning 'Ok(true)'
                        // Consequently, the key remains.
                        // In order to resolve this, check the pointer again.
                        return Err(RemoveError::Retry(removed));
                    }
                    if empty {
                        if !child_ref.retire() {
                            return Err(RemoveError::Retry(removed));
                        }
                        return self.coalesce(removed, barrier);
                    }
                    return Ok(removed);
                }

                // `child_ptr` being null indicates that the leaf node is bound to be freed.
                return Err(RemoveError::Retry(false));
            }
            let unbounded_ptr = (self.leaves.1).load(Acquire, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see LeafNode::search.
                    continue;
                }
                let (removed, full, empty) = unbounded_ref.remove_if(key_ref, condition);
                if full && !self.check_full_leaf(key_ref, unbounded_ptr, barrier) {
                    // Data race resolution - see above.
                    return Err(RemoveError::Retry(removed));
                }
                if empty {
                    if !unbounded_ref.retire() {
                        return Err(RemoveError::Retry(removed));
                    }
                    return self.coalesce(removed, barrier);
                }
                return Ok(removed);
            }
            if unbounded_ptr.tag() == Tag::First {
                // The leaf node has become obsolete.
                return Err(RemoveError::Empty(false));
            }
            // The `TreeIndex` is empty.
            return Ok(false);
        }
    }

    /// Splits a full leaf.
    ///
    /// Returns `true` if the leaf is successfully split or a conflict is detected, false
    /// otherwise.
    fn split_leaf(
        &self,
        full_leaf_key: Option<K>,
        full_leaf_ptr: Ptr<Leaf<K, V>>,
        full_leaf: &AtomicArc<Leaf<K, V>>,
        barrier: &Barrier,
    ) -> bool {
        let new_leaves_ptr;
        match self.new_leaves.compare_exchange(
            Ptr::null(),
            (
                Some(Arc::new(NewLeaves {
                    origin_leaf_key: full_leaf_key,
                    origin_leaf_ptr: full_leaf.clone(Relaxed, barrier),
                    low_key_leaf: AtomicArc::null(),
                    high_key_leaf: AtomicArc::null(),
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
        ) {
            Ok((_, ptr)) => new_leaves_ptr = ptr,
            Err(_) => return true,
        }

        // Checks the full leaf pointer and the leaf node state after locking the leaf node.
        if full_leaf_ptr != full_leaf.load(Relaxed, barrier) {
            drop(self.new_leaves.swap((None, Tag::None), Relaxed));
            return true;
        }

        let full_leaf_ref = full_leaf_ptr.as_ref().unwrap();
        debug_assert!(full_leaf_ref.full());

        // Copies entries to the newly allocated leaves.
        let new_leaves_ref = new_leaves_ptr.as_ref().unwrap();
        let mut low_key_leaf_arc = None;
        let mut high_key_leaf_arc = None;
        full_leaf_ref.distribute(&mut low_key_leaf_arc, &mut high_key_leaf_arc);

        if let Some(low_key_leaf_boxed) = low_key_leaf_arc.take() {
            // The number of valid entries is small enough to fit into a single leaf.
            new_leaves_ref
                .low_key_leaf
                .swap((Some(low_key_leaf_boxed), Tag::None), Relaxed);
            if let Some(high_key_leaf) = high_key_leaf_arc.take() {
                new_leaves_ref
                    .high_key_leaf
                    .swap((Some(high_key_leaf), Tag::None), Relaxed);
            }
        } else {
            // No valid keys in the full leaf.
            new_leaves_ref
                .low_key_leaf
                .swap((Some(Arc::new(Leaf::new())), Tag::None), Relaxed);
        }

        // Inserts the newly added leaves into the main array.
        //  - Inserts the new leaves into the linked list, and removes the full leaf from it.
        let low_key_leaf_ptr = new_leaves_ref.low_key_leaf.load(Relaxed, barrier);
        let high_key_leaf_ptr = new_leaves_ref.high_key_leaf.load(Relaxed, barrier);
        let unused_leaf = if high_key_leaf_ptr.is_null() {
            // From here, Scanners can reach the new leaf.
            let result = full_leaf_ref.push_back(
                low_key_leaf_ptr.try_into_arc().unwrap(),
                true,
                Release,
                barrier,
            );
            debug_assert!(result.is_ok());
            // Replaces the full leaf with the low-key leaf.
            full_leaf.swap((low_key_leaf_ptr.try_into_arc(), Tag::None), Release)
        } else {
            // From here, Scanners can reach the new leaves.
            //
            // Immediately unlinking the full leaf causes active scanners reading the full leaf
            // to omit a number of leaves.
            //  - Leaf: l, insert: i, split: s, rollback: r.
            //  - Insert 1-2: i1(l1)|                   |i2(l2)|s(l2)|l12:l2:l21:l22|l12:l21:l22|
            //  - Insert 0  : i0(l1)|s(l1)|l1:l11:l12:l2|                                       |r|l1:l12...
            // In this scenario, without keeping l1, l2 in the linked list, l1 would point to
            // l2, and therefore a range scanner would start from l1, and cannot traverse l21
            // and l22, missing newly inserted entries in l21 and l22 before starting the
            // range scanner.
            let result = full_leaf_ref.push_back(
                high_key_leaf_ptr.try_into_arc().unwrap(),
                true,
                Release,
                barrier,
            );
            debug_assert!(result.is_ok());
            let result = full_leaf_ref.push_back(
                low_key_leaf_ptr.try_into_arc().unwrap(),
                true,
                Release,
                barrier,
            );
            debug_assert!(result.is_ok());

            // Takes the max key value stored in the low key leaf as the leaf key.
            let max_key = low_key_leaf_ptr.as_ref().unwrap().max().unwrap().0;
            if self
                .leaves
                .0
                .insert(
                    max_key.clone(),
                    new_leaves_ref.low_key_leaf.clone(Relaxed, barrier),
                )
                .is_some()
            {
                // Insertion failed: expects that the parent splits the leaf node.
                return false;
            }

            // Replaces the full leaf with the high-key leaf.
            full_leaf.swap((high_key_leaf_ptr.try_into_arc(), Tag::None), Release)
        };

        // Drops the deprecated leaf.
        if let Some(unused_leaf) = unused_leaf {
            debug_assert!(unused_leaf.full());
            let deleted = unused_leaf.delete_self(Release);
            debug_assert!(deleted);
            barrier.reclaim(unused_leaf);
        }

        // Unlocks the leaf node.
        self.new_leaves.swap((None, Tag::None), Release);

        true
    }

    /// Splits itself into the given leaf nodes, and returns the middle key value.
    pub fn split_leaf_node(
        &self,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        barrier: &Barrier,
    ) -> Option<K> {
        let mut middle_key = None;

        debug_assert!(!self.new_leaves.load(Relaxed, barrier).is_null());
        let new_leaves_ref = self.new_leaves.load(Relaxed, barrier).as_ref().unwrap();

        let low_key_leaves = &low_key_leaf_node.leaves;
        let high_key_leaves = &high_key_leaf_node.leaves;

        // Builds a list of valid leaves
        #[allow(clippy::type_complexity)]
        let mut entry_array: [Option<(Option<&K>, AtomicArc<Leaf<K, V>>)>; ARRAY_SIZE + 2] =
            Default::default();
        let mut num_entries = 0;
        let low_key_leaf_ref = new_leaves_ref
            .low_key_leaf
            .load(Relaxed, barrier)
            .as_ref()
            .unwrap();
        let middle_key_ref = low_key_leaf_ref.max().unwrap().0;
        for entry in Scanner::new(&self.leaves.0) {
            if new_leaves_ref
                .origin_leaf_key
                .as_ref()
                .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
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
            // If the origin is a bounded node, assign the unbounded node to the high key node's unbounded.
            entry_array[num_entries].replace((None, self.leaves.1.clone(Relaxed, barrier)));
            num_entries += 1;
        } else {
            // If the origin is an unbounded node, assign the high key node to the high key node's unbounded.
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
                    Ordering::Less => {
                        low_key_leaves
                            .0
                            .insert(entry.0.unwrap().clone(), entry.1.clone(Relaxed, barrier));
                    }
                    Ordering::Equal => {
                        middle_key.replace(entry.0.unwrap().clone());
                        low_key_leaves
                            .1
                            .swap((entry.1.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                    }
                    Ordering::Greater => {
                        if let Some(key) = entry.0 {
                            high_key_leaves
                                .0
                                .insert(key.clone(), entry.1.clone(Relaxed, barrier));
                        } else {
                            high_key_leaves
                                .1
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

    /// Checks the given full leaf whether it is being split.
    fn check_full_leaf<Q>(&self, key_ref: &Q, leaf_ptr: Ptr<Leaf<K, V>>, barrier: &Barrier) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // There is a chance that the target key value pair has been copied to new_leaves,
        // and the following scenario cannot be prevented by memory fences.
        //  - Remove: release(leaf)|load(mutex_old)|acquire|load(leaf_ptr_old)
        //  - Insert: load(leaf)|store(mutex)|acquire|release|store(leaf_ptr)|release|store(mutex)
        // Therefore, it performs CAS to check the freshness of the pointer value.
        //  - Remove: release(leaf)|cas(mutex)|acquire|release|load(leaf_ptr)
        //  - Insert: load(leaf)|store(mutex)|acquire|release|store(leaf_ptr)|release|store(mutex)
        if self
            .new_leaves
            .compare_exchange(Ptr::null(), (None, Tag::None), AcqRel, Relaxed)
            .is_err()
        {
            return false;
        }
        let result = (self.leaves.0).min_greater_equal(key_ref);
        let leaf_current_ptr = if let Some((_, child)) = result.0 {
            child.load(Relaxed, barrier)
        } else {
            self.leaves.1.load(Relaxed, barrier)
        };
        leaf_current_ptr == leaf_ptr
    }

    /// Tries to coalesce empty or obsolete leaves.
    fn coalesce(&self, removed: bool, barrier: &Barrier) -> Result<bool, RemoveError> {
        let lock = Locker::try_lock(self);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        let mut num_valid_leaves = 0;
        for entry in Scanner::new(&self.leaves.0) {
            let leaf_ptr = entry.1.load(Relaxed, barrier);
            let leaf_ref = leaf_ptr.as_ref().unwrap();
            if leaf_ref.obsolete() {
                // Data race resolution - see LeafScanner::jump.
                let deleted = leaf_ref.delete_self(Relaxed);
                debug_assert!(deleted);
                self.leaves.0.remove_if(entry.0, &mut |_| true);
                // Data race resolution - see LeafNode::search.
                if let Some(leaf) = entry.1.swap((None, Tag::None), Release) {
                    barrier.reclaim(leaf);
                }
            } else {
                num_valid_leaves += 1;
            }
        }

        // Checks the unbounded leaf only when all the other leaves have become obsolete.
        let check_unbounded = if num_valid_leaves == 0 && self.leaves.0.retire() {
            true
        } else {
            self.leaves.0.obsolete()
        };

        // The unbounded leaf becomes unreachable after all the other leaves are gone.
        let fully_empty = if check_unbounded {
            let unbounded_ptr = self.leaves.1.load(Relaxed, barrier);
            if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
                if unbounded_ref.obsolete() {
                    // Data race resolution - see LeafScanner::jump.
                    let deleted = unbounded_ref.delete_self(Relaxed);
                    debug_assert!(deleted);
                    if let Some(obsolete_leaf) = self.leaves.1.swap((None, Tag::First), Release) {
                        barrier.reclaim(obsolete_leaf);
                    }
                    true
                } else {
                    false
                }
            } else {
                debug_assert!(unbounded_ptr.tag() == Tag::First);
                true
            }
        } else {
            false
        };

        if fully_empty {
            Err(RemoveError::Empty(removed))
        } else {
            Ok(removed)
        }
    }

    /// Rolls back the ongoing split operation recursively.
    pub fn rollback(&self, barrier: &Barrier) {
        let new_leaves_ptr = self.new_leaves.load(Relaxed, barrier);
        if let Some(new_leaves_ref) = new_leaves_ptr.as_ref() {
            // Inserts the origin leaf into the linked list.
            let low_key_leaf_ptr = new_leaves_ref.low_key_leaf.load(Relaxed, barrier);
            let high_key_leaf_ptr = new_leaves_ref.high_key_leaf.load(Relaxed, barrier);

            // Rolls back the linked list state.
            //
            // `high_key_leaf` must be deleted first in order for scanners not to omit entries.
            if let Some(leaf_ref) = high_key_leaf_ptr.as_ref() {
                let deleted = leaf_ref.delete_self(Relaxed);
                debug_assert!(deleted);
            }
            if let Some(leaf_ref) = low_key_leaf_ptr.as_ref() {
                let deleted = leaf_ref.delete_self(Release);
                debug_assert!(deleted);
            }

            if let Some(origin_leaf) = new_leaves_ref
                .origin_leaf_ptr
                .swap((None, Tag::None), Relaxed)
            {
                // Remove marks from the full leaf node.
                //
                // This unmarking has to be a release-store, otherwise it can be re-ordered
                // before previous `delete_self` calls.
                let unmarked = origin_leaf.unmark(Release);
                debug_assert!(unmarked);
            }

            // Unlocks the leaf node.
            if let Some(new_leaves) = self.new_leaves.swap((None, Tag::None), Release) {
                barrier.reclaim(new_leaves);
            }
        };
    }

    /// Unlinks all the leaves.
    ///
    /// It is called only when the leaf node is a temporary one for split/merge,
    /// or has become unreachable after split/merge/remove.
    pub fn unlink(&self, barrier: &Barrier) {
        for entry in Scanner::new(&self.leaves.0) {
            entry.1.swap((None, Tag::None), Relaxed);
        }
        self.leaves.1.swap((None, Tag::First), Relaxed);

        // Keeps the leaf node locked to prevent locking attempts.
        if let Some(unused_leaves) = self.new_leaves.swap((None, Tag::First), Relaxed) {
            if let Some(obsolete_leaf) = unused_leaves
                .origin_leaf_ptr
                .swap((None, Tag::None), Relaxed)
            {
                // Makes the leaf unreachable before dropping it.
                let deleted = obsolete_leaf.delete_self(Relaxed);
                debug_assert!(deleted);
                barrier.reclaim(obsolete_leaf);
            }
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> LeafNode<K, V> {
    pub fn print<T: std::io::Write>(
        &self,
        output: &mut T,
        depth: usize,
        barrier: &Barrier,
    ) -> std::io::Result<()> {
        // Collects information.
        #[allow(clippy::type_complexity)]
        let mut leaf_ref_array: [Option<(Option<&Leaf<K, V>>, Option<&K>, usize)>;
            ARRAY_SIZE + 1] = [None; ARRAY_SIZE + 1];
        let mut scanner = Scanner::new_including_removed(&self.leaves.0);
        let mut index = 0;
        while let Some(entry) = scanner.next() {
            if scanner.removed() {
                leaf_ref_array[index].replace((None, Some(entry.0), index));
            } else {
                let leaf_ptr = entry.1.load(Relaxed, barrier);
                leaf_ref_array[index].replace((leaf_ptr.as_ref(), Some(entry.0), index));
            }
            index += 1;
        }
        let unbounded_ptr = self.leaves.1.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            leaf_ref_array[index].replace((Some(unbounded_ref), None, index));
        }

        // Prints the label.
        output.write_fmt(format_args!(
            "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td colspan='{}'>ID: {}, Level: {}, Cardinality: {}</td></tr>\n<tr>",
            self.id(),
            index + 1,
            self.id(),
            depth,
            index + 1,
        ))?;
        #[allow(clippy::manual_flatten)]
        for leaf_info in &leaf_ref_array {
            if let Some((leaf_ref, key_ref, index)) = leaf_info {
                let font_color = if leaf_ref.is_some() { "black" } else { "red" };
                if let Some(key_ref) = key_ref {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>{}</font></td>",
                        index, font_color, key_ref,
                    ))?;
                } else {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>\u{221e}</font></td>",
                        index, font_color,
                    ))?;
                }
            }
        }
        output.write_fmt(format_args!("</tr>\n</table>\n>]\n"))?;

        // Prints the edges and children.
        #[allow(clippy::manual_flatten)]
        for leaf_info in &leaf_ref_array {
            if let Some((Some(leaf_ref), _, index)) = leaf_info {
                output.write_fmt(format_args!(
                    "{}:p_{} -> {}\n",
                    self.id(),
                    index,
                    leaf_ref.id()
                ))?;
                output.write_fmt(format_args!(
                        "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td colspan='3'>ID: {}</td></tr><tr><td>Rank</td><td>Key</td><td>Value</td></tr>\n",
                        leaf_ref.id(),
                        leaf_ref.id(),
                    ))?;
                let mut leaf_scanner = Scanner::new_including_removed(leaf_ref);
                let mut rank = 0;
                while let Some(entry) = leaf_scanner.next() {
                    let (entry_rank, font_color) = if leaf_scanner.removed() {
                        (0, "red")
                    } else {
                        rank += 1;
                        (rank, "black")
                    };
                    output.write_fmt(format_args!(
                        "<tr><td><font color=\"{}\">{}</font></td><td>{}</td><td>{}</td></tr>\n",
                        font_color, entry_rank, entry.0, entry.1,
                    ))?;
                }
                output.write_fmt(format_args!("</table>\n>]\n"))?;
            }
        }

        std::io::Result::Ok(())
    }
}

/// Leaf node locker.
pub struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    lock: &'n AtomicArc<NewLeaves<K, V>>,
    /// When the leaf node is bound to be dropped, the flag may be set true.
    deprecate: bool,
}

impl<'n, K, V> Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    pub fn try_lock(leaf_node: &'n LeafNode<K, V>) -> Option<Locker<'n, K, V>> {
        if leaf_node
            .new_leaves
            .compare_exchange(Ptr::null(), (None, Tag::First), Acquire, Relaxed)
            .is_ok()
        {
            Some(Locker {
                lock: &leaf_node.new_leaves,
                deprecate: false,
            })
        } else {
            None
        }
    }

    pub fn deprecate(&mut self) {
        self.deprecate = true;
    }
}

impl<'n, K, V> Drop for Locker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn drop(&mut self) {
        if !self.deprecate {
            let unlocked = self
                .lock
                .update_tag_if(Tag::None, |t| t == Tag::First, Release);
            debug_assert!(unlocked);
        }
    }
}

/// Intermediate split leaf.
///
/// It owns all the instances, thus dropping them all when it is being dropped.
pub struct NewLeaves<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    origin_leaf_key: Option<K>,
    origin_leaf_ptr: AtomicArc<Leaf<K, V>>,
    low_key_leaf: AtomicArc<Leaf<K, V>>,
    high_key_leaf: AtomicArc<Leaf<K, V>>,
}
