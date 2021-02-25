use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

/// Leaf node.
///
/// The layout of a leaf node: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
pub struct LeafNode<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Child leaves.
    leaves: (Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
    /// New leaves in an intermediate state during merge and split.
    ///
    /// A valid pointer stored in the variable acts as a mutex for merge and split operations.
    new_leaves: Atomic<NewLeaves<K, V>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNode<K, V> {
    pub fn new(allocate_unbounded_leaf: bool) -> LeafNode<K, V> {
        let unbounded_leaf: Atomic<Leaf<K, V>> = if allocate_unbounded_leaf {
            Atomic::from(Owned::new(Leaf::new()))
        } else {
            Atomic::null()
        };
        LeafNode {
            leaves: (Leaf::new(), unbounded_leaf),
            new_leaves: Atomic::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Checks if the internal node is obsolete.
    pub fn obsolete(&self, check_unbounded: bool, guard: &Guard) -> bool {
        if self.leaves.0.obsolete() {
            if check_unbounded {
                let unbounded_shared = self.leaves.1.load(Relaxed, guard);
                if !unbounded_shared.is_null() {
                    return unsafe { unbounded_shared.deref().obsolete() };
                }
                return true;
            }
            return true;
        }
        false
    }

    pub fn detach(&self, guard: &Guard) {
        debug_assert!(self.obsolete(true, guard));
        let _locker = LeafNodeLocker::lock(self, guard);
        let unbounded_shared = self.leaves.1.swap(Shared::null(), Relaxed, guard);
        unsafe {
            unbounded_shared.deref().unlink(guard);
            guard.defer_destroy(unbounded_shared);
        }
    }

    pub fn unlink(&self, guard: &Guard) {
        for entry in LeafScanner::new(&self.leaves.0) {
            entry.1.store(Shared::null(), Relaxed);
        }
        self.leaves.1.store(Shared::null(), Relaxed);

        let unused_leaves = self.new_leaves.load(Relaxed, &guard);
        if !unused_leaves.is_null() {
            let obsolete_leaf =
                unsafe { unused_leaves.deref().origin_leaf_ptr.load(Relaxed, &guard) };
            unsafe {
                guard.defer_destroy(obsolete_leaf);
            }
        }
    }

    pub fn search<'a>(&self, key: &'a K, guard: &'a Guard) -> Result<Option<&'a V>, SearchError> {
        loop {
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata.
                    //  - Writer: start to insert an intermediate low key leaf
                    //  - Reader: read the metadata not including the intermediate low key leaf
                    //  - Writer: insert the intermediate low key leaf and replace the high key leaf pointer
                    //  - Reader: read the new high key leaf pointer
                    // Consequently, the reader may miss keys in the low key leaf.
                    continue;
                }
                if child_leaf.is_null() {
                    // child_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return Ok(unsafe { child_leaf.deref().search(key) });
            }
            let unbounded_shared = (self.leaves.1).load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see above.
                    continue;
                }
                return Ok(unsafe { unbounded_shared.deref().search(key) });
            }
            // unbounded_shared being null indicates that the leaf node is bound to be freed.
            return Err(SearchError::Retry);
        }
    }

    pub fn min<'a>(&'a self, guard: &'a Guard) -> Result<LeafScanner<'a, K, V>, SearchError> {
        loop {
            let mut scanner = LeafScanner::new(&self.leaves.0);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see 'LeafNode::search'.
                    continue;
                }
                if child_leaf.is_null() {
                    // child_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return Ok(LeafScanner::new(unsafe { child_leaf.deref() }));
            }
            let unbounded_shared = (self.leaves.1).load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see above
                    continue;
                }
                return Ok(LeafScanner::new(unsafe { unbounded_shared.deref() }));
            }
            // unbounded_shared being null indicates that the leaf node is bound to be freed.
            return Err(SearchError::Retry);
        }
    }

    pub fn max_less<'a>(
        &'a self,
        key: &K,
        guard: &'a Guard,
    ) -> Result<LeafScanner<'a, K, V>, SearchError> {
        loop {
            let mut scanner = LeafScanner::max_less(&self.leaves.0, key);
            let metadata = scanner.metadata();
            if let Some((_, child)) = scanner.next() {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see 'LeafNode::search'.
                    continue;
                }
                if child_leaf.is_null() {
                    // child_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return Ok(LeafScanner::max_less(unsafe { child_leaf.deref() }, key));
            }
            let unbounded_shared = (self.leaves.1).load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                if !(self.leaves.0).validate(metadata) {
                    // Data race resolution - see above
                    continue;
                }
                return Ok(LeafScanner::max_less(
                    unsafe { unbounded_shared.deref() },
                    key,
                ));
            }
            // unbounded_shared being null indicates that the leaf node is bound to be freed.
            return Err(SearchError::Retry);
        }
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), InsertError<K, V>> {
        loop {
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see 'InternalNode::search'.
                    continue;
                }
                if child_leaf.is_null() {
                    // child_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(InsertError::Retry((key, value)));
                }
                return unsafe { child_leaf.deref().insert(key, value) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(InsertError::Duplicated(result.0));
                        }
                        debug_assert!(unsafe { child_leaf.deref().full() });
                        if !self.split_leaf(Some(child_key.clone()), child_leaf, &child, guard) {
                            return Err(InsertError::Full(result.0));
                        }
                        Err(InsertError::Retry(result.0))
                    },
                );
            }
            let unbounded_shared = self.leaves.1.load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see 'InternalNode::search'.
                    continue;
                }
                // Tries to insert into the unbounded leaf, and tries to split the unbounded if it is full.
                return unsafe { unbounded_shared.deref().insert(key, value) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(InsertError::Duplicated(result.0));
                        }
                        debug_assert!(unsafe { unbounded_shared.deref().full() });
                        if !self.split_leaf(None, unbounded_shared, &self.leaves.1, guard) {
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

    pub fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> Result<bool, RemoveError> {
        loop {
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see 'InternalNode::search'.
                    continue;
                }
                if child_leaf.is_null() {
                    // child_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(RemoveError::Retry(false));
                }
                let child_leaf_ref = unsafe { child_leaf.deref() };
                let (removed, full, empty) = child_leaf_ref.remove(key);
                if !full && !empty {
                    return Ok(removed);
                } else if !empty {
                    // Data race resolution.
                    //  - Insert: start to insert into a full leaf
                    //  - Remove: start removing an entry from the leaf after pointer validation
                    //  - Insert: find the leaf full, thus splitting and update
                    //  - Remove: find the leaf full, and the leaf node is not locked, returning 'Ok(true)'
                    // Consequently, the key remains.
                    // In order to resolve this, check the pointer again.
                    return self.check_full_leaf(removed, key, child_leaf, guard);
                }
                child_leaf_ref.retire();
                return self.coalesce(removed, guard);
            }
            let unbounded_shared = (self.leaves.1).load(Relaxed, guard);
            if !unbounded_shared.is_null() {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution - see 'InternalNode::search'.
                    continue;
                }
                let unbounded_leaf_ref = unsafe { unbounded_shared.deref() };
                let (removed, full, empty) = unbounded_leaf_ref.remove(key);
                if !full && !empty {
                    return Ok(removed);
                } else if !empty {
                    // Data race resolution - see above.
                    return self.check_full_leaf(removed, key, unbounded_shared, guard);
                }
                unbounded_leaf_ref.retire();
                return self.coalesce(removed, guard);
            }
            // unbounded_shared being null indicates that the leaf node is bound to be freed.
            return Err(RemoveError::Retry(false));
        }
    }

    pub fn rollback(&self, guard: &Guard) {
        let new_leaves = self.new_leaves.load(Relaxed, guard);
        unsafe {
            // Inserts the origin leaf into the linked list.
            let origin_leaf_shared = new_leaves.deref().origin_leaf_ptr.load(Relaxed, guard);
            let low_key_leaf_shared = new_leaves.deref().low_key_leaf.load(Relaxed, guard);
            let high_key_leaf_shared = new_leaves.deref().high_key_leaf.load(Relaxed, guard);
            high_key_leaf_shared
                .deref()
                .push_back(origin_leaf_shared.deref(), guard);

            // The leaf with higher keys must be unlinked first.
            high_key_leaf_shared.deref().unlink(guard);
            guard.defer_destroy(high_key_leaf_shared);

            // Unlinks the leaf with lower keys.
            low_key_leaf_shared.deref().unlink(guard);
            guard.defer_destroy(low_key_leaf_shared);

            // Unlocks the leaf node.
            drop(
                self.new_leaves
                    .swap(Shared::null(), Release, guard)
                    .into_owned(),
            );
        };
    }

    /// Splits itself into the given leaf nodes, and returns the middle key value.
    pub fn split_leaf_node(
        &self,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        guard: &Guard,
    ) -> Option<K> {
        let mut middle_key = None;

        debug_assert!(!self.new_leaves.load(Relaxed, guard).is_null());
        let new_leaves_ref = unsafe { self.new_leaves.load(Relaxed, guard).deref_mut() };

        let low_key_leaves = &low_key_leaf_node.leaves;
        let high_key_leaves = &high_key_leaf_node.leaves;

        // Builds a list of valid leaves
        let mut entry_array: [Option<(Option<&K>, Atomic<Leaf<K, V>>)>; ARRAY_SIZE + 2] =
            [None, None, None, None, None, None, None, None, None, None];
        let mut num_entries = 0;
        let low_key_leaf_ref = unsafe { new_leaves_ref.low_key_leaf.load(Relaxed, guard).deref() };
        let middle_key_ref = low_key_leaf_ref.max().unwrap().0;
        for entry in LeafScanner::new(&self.leaves.0) {
            if new_leaves_ref
                .origin_leaf_key
                .as_ref()
                .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
            {
                entry_array[num_entries]
                    .replace((Some(middle_key_ref), new_leaves_ref.low_key_leaf.clone()));
                num_entries += 1;
                if !new_leaves_ref.high_key_leaf.load(Relaxed, guard).is_null() {
                    entry_array[num_entries]
                        .replace((Some(entry.0), new_leaves_ref.high_key_leaf.clone()));
                    num_entries += 1;
                }
            } else {
                entry_array[num_entries].replace((Some(entry.0), entry.1.clone()));
                num_entries += 1;
            }
        }
        if new_leaves_ref.origin_leaf_key.is_some() {
            // If the origin is a bounded node, assign the unbounded node to the high key node's unbounded.
            entry_array[num_entries].replace((None, self.leaves.1.clone()));
            num_entries += 1;
        } else {
            // If the origin is an unbounded node, assign the high key node to the high key node's unbounded.
            entry_array[num_entries]
                .replace((Some(middle_key_ref), new_leaves_ref.low_key_leaf.clone()));
            num_entries += 1;
            if !new_leaves_ref.high_key_leaf.load(Relaxed, guard).is_null() {
                entry_array[num_entries].replace((None, new_leaves_ref.high_key_leaf.clone()));
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
                            .insert(entry.0.unwrap().clone(), entry.1.clone());
                    }
                    Ordering::Equal => {
                        middle_key.replace(entry.0.unwrap().clone());
                        low_key_leaves
                            .1
                            .store(entry.1.load(Relaxed, guard), Relaxed);
                    }
                    Ordering::Greater => {
                        if let Some(key) = entry.0 {
                            high_key_leaves.0.insert(key.clone(), entry.1.clone());
                        } else {
                            high_key_leaves
                                .1
                                .store(entry.1.load(Relaxed, guard), Relaxed);
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

    /// Splits a full leaf.
    ///
    /// Returns true if the leaf is successfully split or a conflict is detected, false otherwise.
    fn split_leaf(
        &self,
        full_leaf_key: Option<K>,
        full_leaf_shared: Shared<Leaf<K, V>>,
        full_leaf_ptr: &Atomic<Leaf<K, V>>,
        guard: &Guard,
    ) -> bool {
        let mut new_leaves_ptr;
        match self.new_leaves.compare_exchange(
            Shared::null(),
            Owned::new(NewLeaves {
                origin_leaf_key: full_leaf_key,
                origin_leaf_ptr: full_leaf_ptr.clone(),
                low_key_leaf: Atomic::null(),
                high_key_leaf: Atomic::null(),
            }),
            Acquire,
            Relaxed,
            guard,
        ) {
            Ok(result) => new_leaves_ptr = result,
            Err(_) => return true,
        }

        // Checks the full leaf pointer and the leaf node state after locking the leaf node.
        if full_leaf_shared != full_leaf_ptr.load(Relaxed, guard) {
            let obsolete_leaf = self.new_leaves.swap(Shared::null(), Relaxed, guard);
            drop(unsafe { obsolete_leaf.into_owned() });
            return true;
        }

        let full_leaf_ref = unsafe { full_leaf_shared.deref() };
        debug_assert!(full_leaf_ref.full());

        // Copies entries to the newly allocated leaves.
        let new_leaves_ref = unsafe { new_leaves_ptr.deref_mut() };
        let mut low_key_leaf_boxed = None;
        let mut high_key_leaf_boxed = None;
        full_leaf_ref.distribute(&mut low_key_leaf_boxed, &mut high_key_leaf_boxed);

        if let Some(low_key_leaf_boxed) = low_key_leaf_boxed.take() {
            // The number of valid entries is small enough to fit into a single leaf.
            new_leaves_ref.low_key_leaf = Atomic::from(low_key_leaf_boxed);
            if let Some(high_key_leaf) = high_key_leaf_boxed.take() {
                new_leaves_ref.high_key_leaf = Atomic::from(high_key_leaf);
            }
        } else {
            // No valid keys in the full leaf.
            new_leaves_ref.low_key_leaf = Atomic::from(Owned::new(Leaf::new()));
        }

        // Inserts the newly added leaves into the main array.
        //  - Inserts the new leaves into the linked list, and removes the full leaf from it.
        let low_key_leaf_shared = new_leaves_ref.low_key_leaf.load(Relaxed, guard);
        let low_key_leaf_ref = unsafe { low_key_leaf_shared.deref() };
        let high_key_leaf_shared = new_leaves_ref.high_key_leaf.load(Relaxed, guard);
        let unused_leaf = if !high_key_leaf_shared.is_null() {
            let high_key_leaf_ref = unsafe { high_key_leaf_shared.deref() };
            // From here, Scanners can reach the new leaves.
            full_leaf_ref.push_back(low_key_leaf_ref, guard);
            low_key_leaf_ref.push_back(high_key_leaf_ref, guard);
            // From here, Scanners cannot reach the full leaf.
            full_leaf_ref.unlink(guard);
            // Takes the max key value stored in the low key leaf as the leaf key.
            let max_key = low_key_leaf_ref.max().unwrap().0;
            if self
                .leaves
                .0
                .insert(max_key.clone(), new_leaves_ref.low_key_leaf.clone())
                .is_some()
            {
                // Insertion failed: expects that the parent splits the leaf node.
                return false;
            }

            // Replaces the full leaf with the high-key leaf.
            full_leaf_ptr.swap(high_key_leaf_shared, Release, &guard)
        } else {
            // From here, Scanners can reach the new leaves.
            full_leaf_ref.push_back(low_key_leaf_ref, guard);
            // From here, Scanners cannot reach the full leaf.
            full_leaf_ref.unlink(guard);
            // Replaces the full leaf with the low-key leaf.
            full_leaf_ptr.swap(low_key_leaf_shared, Release, &guard)
        };

        // Drops the deprecated leaf.
        unsafe {
            // Unlinks the full leaf before unlocking the leaf node.
            debug_assert!(unused_leaf.deref().full());
            guard.defer_destroy(unused_leaf);

            // Unlocks the leaf node.
            let unused_leaves = self.new_leaves.swap(Shared::null(), Release, guard);
            let new_split_leaves = unused_leaves.into_owned();
            debug_assert_eq!(
                new_split_leaves.origin_leaf_ptr.load(Relaxed, guard),
                unused_leaf
            );
        };
        true
    }

    fn check_full_leaf(
        &self,
        removed: bool,
        key: &K,
        leaf_shared: Shared<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<bool, RemoveError> {
        // There is a chance that the target key value pair has been copied to new_leaves,
        // and the following scenario cannot be prevented by memory fences.
        //  - Remove: release(leaf)|load(mutex_old)|acquire|load(leaf_ptr_old)
        //  - Insert: load(leaf)|store(mutex)|acquire|release|store(leaf_ptr)|release|store(mutex)
        // Therefore, it performs CAS to check the freshness of the pointer value.
        //  - Remove: release(leaf)|cas(mutex)|acquire|release|load(leaf_ptr)
        //  - Insert: load(leaf)|store(mutex)|acquire|release|store(leaf_ptr)|release|store(mutex)
        if self
            .new_leaves
            .compare_exchange(Shared::null(), Shared::null(), AcqRel, Relaxed, guard)
            .is_err()
        {
            return Err(RemoveError::Retry(removed));
        }
        let result = (self.leaves.0).min_greater_equal(&key);
        let leaf_current_shared = if let Some((_, child)) = result.0 {
            child.load(Relaxed, guard)
        } else {
            self.leaves.1.load(Relaxed, guard)
        };
        if leaf_current_shared != leaf_shared {
            Err(RemoveError::Retry(removed))
        } else {
            Ok(removed)
        }
    }

    /// Tries to coalesce empty or obsolete leaves.
    fn coalesce(&self, removed: bool, guard: &Guard) -> Result<bool, RemoveError> {
        let lock = LeafNodeLocker::try_lock(self, guard);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        let mut empty = false;
        for entry in LeafScanner::new(&self.leaves.0) {
            let leaf_shared = entry.1.load(Relaxed, guard);
            let leaf_ref = unsafe { leaf_shared.deref() };
            if leaf_ref.obsolete() {
                empty = self.leaves.0.remove(entry.0).2;
                // Once the key is removed, it is safe to deallocate the node as the validation loop ensures the absence of readers.
                entry.1.store(Shared::null(), Release);
                unsafe {
                    leaf_ref.unlink(guard);
                    guard.defer_destroy(leaf_shared);
                }
            }
        }
        if empty && self.leaves.0.retire() {
            return Err(RemoveError::Coalesce(removed));
        }

        if self.leaves.0.obsolete() {
            Err(RemoveError::Coalesce(removed))
        } else {
            Ok(removed)
        }
    }
}

impl<K: Clone + Display + Ord + Send + Sync, V: Clone + Display + Send + Sync> LeafNode<K, V> {
    pub fn print<T: std::io::Write>(&self, output: &mut T, guard: &Guard) -> std::io::Result<()> {
        // Collects information.
        let mut leaf_ref_array: [Option<(Option<&Leaf<K, V>>, Option<&K>, usize)>; ARRAY_SIZE + 1] =
            [None; ARRAY_SIZE + 1];
        let mut scanner = LeafScanner::new_including_removed(&self.leaves.0);
        let mut index = 0;
        while let Some(entry) = scanner.next() {
            if !scanner.removed() {
                let leaf_share_ptr = entry.1.load(Relaxed, &guard);
                let leaf_ref = unsafe { leaf_share_ptr.deref() };
                leaf_ref_array[index].replace((Some(leaf_ref), Some(entry.0), index));
            } else {
                leaf_ref_array[index].replace((None, Some(entry.0), index));
            }
            index += 1;
        }
        let unbounded_shared = self.leaves.1.load(Relaxed, &guard);
        if !unbounded_shared.is_null() {
            let leaf_ref = unsafe { unbounded_shared.deref() };
            leaf_ref_array[index].replace((Some(leaf_ref), None, index));
        }

        // Prints the label.
        output.write_fmt(format_args!(
            "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td colspan='{}'>ID: {}, Level: 0, Cardinality: {}</td></tr>\n<tr>",
            self.id(),
            index + 1,
            self.id(),
            index + 1,
        ))?;
        for leaf_info in leaf_ref_array.iter() {
            if let Some((leaf_ref, key_ref, index)) = leaf_info {
                let font_color = if leaf_ref.is_some() { "black" } else { "red" };
                if let Some(key_ref) = key_ref {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>{}</font></td>",
                        index, font_color, key_ref,
                    ))?;
                } else {
                    output.write_fmt(format_args!(
                        "<td port='p_{}'><font color='{}'>∞</font></td>",
                        index, font_color,
                    ))?;
                }
            }
        }
        output.write_fmt(format_args!("</tr>\n</table>\n>]\n"))?;

        // Prints the edges and children.
        for leaf_info in leaf_ref_array.iter() {
            if let Some((Some(leaf_ref), _, index)) = leaf_info {
                output.write_fmt(format_args!(
                    "{}:p_{} -> {}\n",
                    self.id(),
                    index,
                    leaf_ref.id()
                ))?;
                output.write_fmt(format_args!(
                        "{} [shape=plaintext\nlabel=<\n<table border='1' cellborder='1'>\n<tr><td colspan='3'>ID: {}</td></tr>¬<tr><td>Rank</td><td>Key</td><td>Value</td></tr>\n",
                        leaf_ref.id(),
                        leaf_ref.id(),
                    ))?;
                let mut leaf_scanner = LeafScanner::new_including_removed(leaf_ref);
                let mut rank = 0;
                while let Some(entry) = leaf_scanner.next() {
                    let (entry_rank, font_color) = if !leaf_scanner.removed() {
                        rank += 1;
                        (rank, "black")
                    } else {
                        (0, "red")
                    };
                    output.write_fmt(format_args!(
                        "<tr><td><font color=\"{}\">{}</font></td><td>{}</td><td>{}</td></tr>\n",
                        font_color, entry_rank, entry.0, entry.1,
                    ))?;
                }
                output.write_fmt(format_args!("</table>\n>]\n"))?;
                let prev_leaf = leaf_ref.backward_link(guard);
                if !prev_leaf.is_null() {
                    output.write_fmt(format_args!("{} -> {}\n", leaf_ref.id(), unsafe {
                        prev_leaf.deref().id()
                    }))?;
                }
                let next_leaf = leaf_ref.forward_link(guard);
                if !next_leaf.is_null() {
                    output.write_fmt(format_args!("{} -> {}\n", leaf_ref.id(), unsafe {
                        next_leaf.deref().id()
                    }))?;
                }
            }
        }

        std::io::Result::Ok(())
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for LeafNode<K, V> {
    fn drop(&mut self) {
        // The leaf node has become unreachable, and so have all the children, therefore pinning is unnecessary.
        for entry in LeafScanner::new(&self.leaves.0) {
            let child = entry
                .1
                .load(Acquire, unsafe { crossbeam_epoch::unprotected() });
            if !child.is_null() {
                unsafe {
                    let leaf = child.into_owned();
                    leaf.unlink(crossbeam_epoch::unprotected());
                }
            }
        }
        let unbounded_shared = self
            .leaves
            .1
            .load(Acquire, unsafe { crossbeam_epoch::unprotected() });
        if !unbounded_shared.is_null() {
            unsafe {
                let leaf = unbounded_shared.into_owned();
                leaf.unlink(crossbeam_epoch::unprotected());
            }
        }
    }
}

/// Leaf node locker.
pub struct LeafNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    lock: &'a Atomic<NewLeaves<K, V>>,
}

impl<'a, K, V> LeafNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn try_lock(
        leaf_node: &'a LeafNode<K, V>,
        guard: &Guard,
    ) -> Option<LeafNodeLocker<'a, K, V>> {
        let mut new_nodes_dummy = NewLeaves::new();
        if let Err(error) = leaf_node.new_leaves.compare_exchange(
            Shared::null(),
            unsafe { Owned::from_raw(&mut new_nodes_dummy as *mut NewLeaves<K, V>) },
            Acquire,
            Relaxed,
            guard,
        ) {
            error.new.into_shared(guard);
            None
        } else {
            Some(LeafNodeLocker {
                lock: &leaf_node.new_leaves,
            })
        }
    }

    pub fn lock(leaf_node: &'a LeafNode<K, V>, guard: &Guard) -> LeafNodeLocker<'a, K, V> {
        loop {
            if let Some(leaf_node_locker) = LeafNodeLocker::try_lock(leaf_node, guard) {
                return leaf_node_locker;
            }
        }
    }
}

impl<'a, K, V> Drop for LeafNodeLocker<'a, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn drop(&mut self) {
        self.lock.store(Shared::null(), Release);
    }
}

/// Intermediate split leaf.
///
/// It owns all the instances, thus dropping them all when it is being dropped.
pub struct NewLeaves<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    origin_leaf_key: Option<K>,
    origin_leaf_ptr: Atomic<Leaf<K, V>>,
    low_key_leaf: Atomic<Leaf<K, V>>,
    high_key_leaf: Atomic<Leaf<K, V>>,
}

impl<K, V> NewLeaves<K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn new() -> NewLeaves<K, V> {
        NewLeaves {
            origin_leaf_key: None,
            origin_leaf_ptr: Atomic::null(),
            low_key_leaf: Atomic::null(),
            high_key_leaf: Atomic::null(),
        }
    }
}
