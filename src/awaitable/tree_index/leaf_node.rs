use super::leaf::{InsertResult, RemoveResult, Scanner};
use super::Leaf;

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::LinkedList;

use std::borrow::Borrow;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};

/// [`Tag::First`] indicates the corresponding [`LeafNode`] has retired.
const RETIRED: Tag = Tag::First;

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
        }
    }

    /// Returns if the [`LeafNode`] is obsolete.
    pub fn obsolete(&self, mo: Ordering) -> bool {
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
                let child_ptr = child.load(Acquire, barrier);
                if !self.children.validate(metadata) {
                    // Data race with split.
                    //  - Writer: start to insert an intermediate low key leaf.
                    //  - Reader: read the metadata not including the intermediate low key leaf.
                    //  - Writer: insert the intermediate low key leaf.
                    //  - Writer: replace the high key leaf pointer.
                    //  - Reader: read the new high key leaf pointer
                    // Consequently, the reader may miss keys in the low key leaf.
                    //
                    // Resolution: metadata validation.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return child_ref.search(key);
                }
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
            if let Some(child) =
                scanner.find_map(|(_, child)| child.load(Acquire, barrier).as_ref())
            {
                // It is necessary to read the metadata of the child leaf prior to checking the
                // `LeafNode` metadata, since, when a leaf is split, the low key leaf will be
                // inserted into `self.children`, therefore the metadata shall change before the
                // full leaf is being deprecated.
                let leaf_scanner = Scanner::new(child);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                return Some(leaf_scanner);
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                let leaf_scanner = Scanner::new(unbounded);
                if !self.children.validate(metadata) {
                    continue;
                }
                return Some(leaf_scanner);
            }
            return None;
        }
    }

    /// Returns an entry with the maximum key among those keys smaller than the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if retry is required: TODO - remove it.
    pub fn max_less<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Result<Scanner<'b, K, V>, ()>
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let mut scanner = Scanner::max_less(&self.children, key);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_ptr = child.load(Acquire, barrier);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    return Ok(Scanner::max_less(child_ref, key));
                }
                // `child_ptr` being null indicates that the leaf is being removed.
                return Err(());
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if !self.children.validate(metadata) {
                    continue;
                }
                return Ok(Scanner::max_less(unbounded, key));
            }
            return Err(());
        }
    }

    /// Inserts a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required.
    pub fn insert(
        &self,
        key: K,
        value: V,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        loop {
            let (child, metadata) = self.children.min_greater_equal(&key);
            if let Some((child_key, child)) = child {
                let child_ptr = child.load(Acquire, barrier);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    match child_ref.insert(key, value) {
                        InsertResult::Success => return Ok(InsertResult::Success),
                        InsertResult::Duplicate(key, value) => {
                            return Ok(InsertResult::Duplicate(key, value));
                        }
                        InsertResult::Full(key, value) | InsertResult::Retired(key, value) => {
                            return self.split_leaf(
                                key,
                                value,
                                Some(child_key),
                                child_ptr,
                                child,
                                barrier,
                            );
                        }
                    };
                }
                // `child_ptr` being null indicates that the leaf is being removed.
                return Err((key, value));
            }

            let mut unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            while unbounded_ptr.is_null() {
                if unbounded_ptr.tag() == RETIRED {
                    return Ok(InsertResult::Retired(key, value));
                }
                match self.unbounded_child.compare_exchange(
                    Ptr::null(),
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
                        return self.split_leaf(
                            key,
                            value,
                            None,
                            unbounded_ptr,
                            &self.unbounded_child,
                            barrier,
                        );
                    }
                };
            }
            return Err((key, value));
        }
    }
    /// Removes an entry associated with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required with a boolean flag indicating that an entry has been removed.
    pub fn remove_if<Q, F: FnMut(&V) -> bool>(
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
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                if let Some(child_ref) = child_ptr.as_ref() {
                    let result = child_ref.remove_if(key, condition);
                    if result == RemoveResult::Fail {
                        return Ok(RemoveResult::Fail);
                    }
                    if result == RemoveResult::Retired {
                        return self.coalesce(barrier);
                    }
                    return self.check_full_leaf(key, child_ptr, barrier);
                }
                // `child_ptr` being null indicates that the leaf is being removed.
                return Err(false);
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !self.children.validate(metadata) {
                    // Data race resolution - see `LeafNode::search`.
                    continue;
                }
                let result = unbounded.remove_if(key, condition);
                if result == RemoveResult::Fail {
                    return Ok(RemoveResult::Fail);
                }
                if result == RemoveResult::Retired {
                    return self.coalesce(barrier);
                }
                return self.check_full_leaf(key, unbounded_ptr, barrier);
            }
            return Err(false);
        }
    }

    /// Splits a full leaf.
    ///
    /// # Errors
    ///
    /// Returns an error if retry is required.
    fn split_leaf(
        &self,
        key: K,
        value: V,
        full_leaf_key: Option<&K>,
        full_leaf_ptr: Ptr<Leaf<K, V>>,
        full_leaf_arc: &AtomicArc<Leaf<K, V>>,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        let new_leaves = match self.latch.compare_exchange(
            Ptr::null(),
            (
                Some(Arc::new(StructuralChange {
                    origin_leaf_key: None,
                    origin_leaf_ptr: full_leaf_arc.clone(Relaxed, barrier),
                    low_key_leaf: AtomicArc::null(),
                    high_key_leaf: AtomicArc::null(),
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
        ) {
            Ok((_, ptr)) => {
                if self.obsolete(Relaxed) || full_leaf_ptr != full_leaf_arc.load(Relaxed, barrier) {
                    // `self` is now obsolete, or `full_leaf` has changed in the meantime.
                    drop(self.latch.swap((None, Tag::None), Relaxed));
                    return Err((key, value));
                }
                ptr.as_ref().unwrap()
            }
            Err(_) => return Err((key, value)),
        };
        if let Some(full_leaf_key) = full_leaf_key {
            let ptr = &new_leaves.origin_leaf_key as *const Option<K> as *mut Option<K>;
            unsafe {
                ptr.write(Some(full_leaf_key.clone()));
            }
        }

        let full_leaf = full_leaf_ptr.as_ref().unwrap();
        let mut low_key_leaf_arc = None;
        let mut high_key_leaf_arc = None;

        // Distribute entries to two leaves.
        full_leaf.distribute(&mut low_key_leaf_arc, &mut high_key_leaf_arc);

        if let Some(low_key_leaf_boxed) = low_key_leaf_arc.take() {
            new_leaves
                .low_key_leaf
                .swap((Some(low_key_leaf_boxed), Tag::None), Relaxed);
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

        // Insert the newly added leaves into the main array, and insert the new leaves into the
        // linked list, and lastly, remove the full leaf from the linked list.
        //
        // When a new leaf is added to the linked list, the leaf is marked to let `Scanners`
        // acknowledge that the newly added leaf may contain keys that are smaller than those
        // having been `scanned`.
        let low_key_leaf_ptr = new_leaves.low_key_leaf.load(Relaxed, barrier);
        let high_key_leaf_ptr = new_leaves.high_key_leaf.load(Relaxed, barrier);
        let unused_leaf = if high_key_leaf_ptr.is_null() {
            // From here, `Scanners` can reach the new leaf.
            //
            // The full leaf is marked so that readers know that the next leaves may contain
            // smaller keys.
            let result =
                full_leaf.push_back(low_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());
            full_leaf_arc.swap(
                (
                    new_leaves.low_key_leaf.swap((None, Tag::None), Relaxed),
                    Tag::None,
                ),
                Release,
            )
        } else {
            // From here, Scanners can reach the new leaves.
            //
            // Immediately unlinking the full leaf causes active scanners reading the full leaf
            // to omit a number of leaves.
            //  - Leaf: l, insert: i, split: s, rollback: r.
            //  - Insert 1-2: i1(l1)|                   |i2(l2)|s(l2)|l12:l2:l21:l22|l12:l21:l22|
            //  - Insert 0  : i0(l1)|s(l1)|l1:l11:l12:l2|                                       |r|l1:l12...
            //
            // In this scenario, without keeping l1, l2 in the linked list, l1 would point to l2,
            // and therefore a range scanner would start from l1, and cannot traverse l21 and l22,
            // missing newly inserted entries in l21 and l22 before starting the range scanner.
            let result =
                full_leaf.push_back(high_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());
            let result =
                full_leaf.push_back(low_key_leaf_ptr.get_arc().unwrap(), true, Release, barrier);
            debug_assert!(result.is_ok());

            // Takes the max key value stored in the low key leaf as the leaf key.
            let max_key = low_key_leaf_ptr.as_ref().unwrap().max().unwrap().0;
            match self.children.insert(
                max_key.clone(),
                new_leaves.low_key_leaf.clone(Relaxed, barrier),
            ) {
                InsertResult::Success => (),
                InsertResult::Duplicate(_, _) => debug_assert!(false, "unreachable"),
                InsertResult::Full(_, _) | InsertResult::Retired(_, _) => {
                    return Ok(InsertResult::Full(key, value))
                }
            };

            // Replace the full leaf with the high-key leaf.
            full_leaf_arc.swap(
                (
                    new_leaves.high_key_leaf.swap((None, Tag::None), Relaxed),
                    Tag::None,
                ),
                Release,
            )
        };

        // Drops the deprecated leaf.
        if let Some(unused_leaf) = unused_leaf {
            let deleted = unused_leaf.delete_self(Release);
            debug_assert!(deleted);
            barrier.reclaim(unused_leaf);
        }

        // Unlocks the leaf node.
        self.latch.swap((None, Tag::None), Release);

        // Since a new leaf has been inserted, the caller can retry.
        Err((key, value))
    }

    /*
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

            let low_key_leaves = &low_key_leaf_node.children;
            let high_key_leaves = &high_key_leaf_node.children;

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
            for entry in Scanner::new(&self.children.0) {
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
                entry_array[num_entries].replace((None, self.children.1.clone(Relaxed, barrier)));
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
    */

    /// Checks the given full leaf whether it is being split.
    fn check_full_leaf<Q>(
        &self,
        key_ref: &Q,
        leaf_ptr: Ptr<Leaf<K, V>>,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // Data race with insert.
        //  - Insert: start to insert into a full leaf
        //  - Remove: start removing an entry from the leaf after pointer validation
        //  - Insert: find the leaf full, thus splitting and update
        //  - Remove: find the leaf full, and the leaf node is not locked, returning 'Ok(true)'
        // Consequently, the key remains.
        //
        // In order to resolve this, check the pointer again.
        let latch = self.latch.load(Acquire, barrier);
        if !latch.is_null() || latch.tag() != Tag::None {
            // The `LeafNode` is being modified.
            return Err(true);
        }
        let (child, _) = self.children.min_greater_equal(key_ref);
        let current_leaf_ptr = if let Some((_, child)) = child {
            child.load(Relaxed, barrier)
        } else {
            self.unbounded_child.load(Relaxed, barrier)
        };
        if current_leaf_ptr == leaf_ptr {
            Ok(RemoveResult::Success)
        } else {
            Err(true)
        }
    }

    /// Tries to coalesce empty or obsolete leaves after a successful removal of an entry.
    fn coalesce(&self, barrier: &Barrier) -> Result<RemoveResult, bool> {
        let lock = Locker::try_lock(self);
        if lock.is_none() {
            return Err(true);
        }

        let mut num_valid_leaves = 0;
        for entry in Scanner::new(&self.children) {
            let leaf_ptr = entry.1.load(Relaxed, barrier);
            let leaf_ref = leaf_ptr.as_ref().unwrap();
            if leaf_ref.retired() {
                let deleted = leaf_ref.delete_self(Relaxed);
                debug_assert!(deleted);
                let result = self.children.remove_if(entry.0, &mut |_| true);
                debug_assert_ne!(result, RemoveResult::Fail);
                if let Some(leaf) = entry.1.swap((None, Tag::None), Release) {
                    barrier.reclaim(leaf);
                }
            } else {
                num_valid_leaves += 1;
            }
        }

        // The unbounded leaf becomes unreachable after all the other leaves are gone.
        let fully_empty = if num_valid_leaves == 0 {
            let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.retired() {
                    let deleted = unbounded.delete_self(Relaxed);
                    debug_assert!(deleted);
                    // It has to mark the pointer in order to prevent `LeafNode::insert` from
                    // replacing it with a new `Leaf`.
                    if let Some(obsolete_leaf) =
                        self.unbounded_child.swap((None, Tag::First), Release)
                    {
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
            Ok(RemoveResult::Retired)
        } else {
            Ok(RemoveResult::Success)
        }
    }

    /*
        /// Rolls back the ongoing split operation recursively.
        pub fn rollback(&self, barrier: &Barrier) {
            let new_leaves_ptr = self.latch.load(Relaxed, barrier);
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
                if let Some(new_leaves) = self.latch.swap((None, Tag::None), Release) {
                    barrier.reclaim(new_leaves);
                }
            };
        }
    */

    /*
        /// Unlinks all the leaves.
        ///
        /// It is called only when the leaf node is a temporary one for split/merge,
        /// or has become unreachable after split/merge/remove.
        pub fn unlink(&self, barrier: &Barrier) {
            for entry in Scanner::new(&self.children.0) {
                entry.1.swap((None, Tag::None), Relaxed);
            }
            self.children.1.swap((None, Tag::First), Relaxed);

            // Keeps the leaf node locked to prevent locking attempts.
            if let Some(unused_leaves) = self.latch.swap((None, Tag::First), Relaxed) {
                if let Some(obsolete_leaf) = unused_leaves
                    .origin_leaf_ptr
                    .swap((None, Tag::None), Relaxed)
                {
                    // Makes the leaf unreachable before dropping it.
                    obsolete_leaf.delete_self(Relaxed);
                    barrier.reclaim(obsolete_leaf);
                }
            }
        }
    */
}

/// Leaf node locker.
pub struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    lock: &'n AtomicArc<StructuralChange<K, V>>,
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
            .latch
            .compare_exchange(Ptr::null(), (None, Tag::First), Acquire, Relaxed)
            .is_ok()
        {
            Some(Locker {
                lock: &leaf_node.latch,
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

/// [`StructuralChange`] stores intermediate results during a split/merge operation.
pub struct StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    origin_leaf_key: Option<K>,
    origin_leaf_ptr: AtomicArc<Leaf<K, V>>,
    low_key_leaf: AtomicArc<Leaf<K, V>>,
    high_key_leaf: AtomicArc<Leaf<K, V>>,
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use proptest::prelude::*;
    use tokio::sync;

    #[test]
    fn basic() {
        let barrier = Barrier::new();
        let leaf_node: LeafNode<String, String> = LeafNode::new();
        assert!(matches!(
            leaf_node.insert(
                "MY GOODNESS!".to_owned(),
                "OH MY GOD!!".to_owned(),
                &barrier
            ),
            Ok(InsertResult::Success)
        ));
        assert!(matches!(
            leaf_node.insert("GOOD DAY".to_owned(), "OH MY GOD!!".to_owned(), &barrier),
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
            leaf_node.remove_if("GOOD DAY", &mut |v| v == "OH MY", &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if("GOOD DAY", &mut |v| v == "OH MY GOD!!", &barrier),
            Ok(RemoveResult::Success)
        ));
        assert!(matches!(
            leaf_node.remove_if("GOOD", &mut |v| v == "OH MY", &barrier),
            Ok(RemoveResult::Fail)
        ));
        assert!(matches!(
            leaf_node.remove_if("MY GOODNESS!", &mut |_| true, &barrier),
            Ok(RemoveResult::Retired)
        ));
        assert!(matches!(
            leaf_node.insert("HI".to_owned(), "HO".to_owned(), &barrier),
            Ok(InsertResult::Retired(..))
        ));
    }

    proptest! {
        #[test]
        fn prop(_insert in 0_usize..256, _remove in 0_usize..256) {
            let _leaf_node: LeafNode<usize, usize> = LeafNode::new();
        }
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 8_usize;
        let workload_size = 256_usize;
        for k in 0..workload_size {
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
                        if let Ok(InsertResult::Success) = leaf_node_clone.insert(k, k, &barrier) {
                            assert!(!inserted_clone.swap(true, Relaxed));
                        }
                    }
                    {
                        barrier_clone.wait().await;
                        let barrier = Barrier::new();
                        for i in 0..workload_size {
                            if i != k {
                                let _result = leaf_node_clone.insert(i, i, &barrier);
                            }
                            assert_eq!(leaf_node_clone.search(&k, &barrier).unwrap(), &k);
                            // TODO: check.
                        }
                        for i in 0..workload_size {
                            let mut scanner = leaf_node_clone.min(&barrier).unwrap();
                            if let Some((k_ref, v_ref)) = scanner.next() {
                                assert_eq!(*k_ref, *v_ref);
                                assert!(*k_ref <= k);
                            } else {
                                let (k_ref, v_ref) =
                                    scanner.jump(None, &barrier).unwrap().get().unwrap();
                                assert_eq!(*k_ref, *v_ref);
                                assert!(*k_ref <= k);
                            }
                            let _result = leaf_node_clone.remove_if(&i, &mut |v| *v != k, &barrier);
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
