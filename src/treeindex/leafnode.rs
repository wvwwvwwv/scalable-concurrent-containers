use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::Leaf;
use super::{InsertError, RemoveError, SearchError};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// Leaf node.
///
/// The layout of a leaf node: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
pub struct LeafNode<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Child leaves.
    ///
    /// A null pointer stored in the variable acts as a mutex for scan, merge, and split operations.
    leaves: (Leaf<K, Atomic<Leaf<K, V>>>, Atomic<Leaf<K, V>>),
    /// New leaves in an intermediate state during merge and split.
    new_leaves: Atomic<NewLeaves<K, V>>,
    /// A pointer that points to a node anchor.
    next_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
    /// A pointer that points to an adjacent leaf node.
    side_link: Atomic<LeafNode<K, V>>,
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
            next_node_anchor: Atomic::null(),
            side_link: Atomic::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    /// Checks if the internal node is obsolete.
    pub fn obsolete(&self, check_unbounded: bool, guard: &Guard) -> bool {
        self.leaves.0.obsolete()
            && (!check_unbounded || self.leaves.1.load(Relaxed, guard).is_null())
    }

    pub fn unlink(&self) {
        for entry in LeafScanner::new(&self.leaves.0) {
            entry.1.store(Shared::null(), Relaxed);
        }
        self.leaves.1.store(Shared::null(), Relaxed);
    }

    pub fn search<'a>(&self, key: &'a K, guard: &'a Guard) -> Result<Option<&'a V>, SearchError> {
        loop {
            let unbounded_leaf = (self.leaves.1).load(Relaxed, guard);
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((_, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata.
                    //  - writer: start to insert an intermediate low key leaf
                    //  - reader: read the metadata not including the intermediate low key leaf
                    //  - writer: insert the intermediate low key leaf and replace the high key leaf pointer
                    //  - reader: read the new high key leaf pointer
                    // Consequently, the reader may miss keys in the low key leaf.
                    continue;
                }
                return Ok(unsafe { child_leaf.deref().search(key) });
            } else if unbounded_leaf == self.leaves.1.load(Acquire, guard) {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata - see above
                    continue;
                }
                if unbounded_leaf.is_null() {
                    // unbounded_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(SearchError::Retry);
                }
                return Ok(unsafe { unbounded_leaf.deref().search(key) });
            }
        }
    }

    pub fn min<'a>(&'a self, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        Some(LeafNodeScanner::new(None, self, guard))
    }

    pub fn from<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        LeafNodeScanner::from(key, self, guard)
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), InsertError<K, V>> {
        loop {
            let unbounded_leaf = self.leaves.1.load(Relaxed, guard);
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                return unsafe { child_leaf.deref().insert(key, value, false) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(InsertError::Duplicated(result.0));
                        }
                        debug_assert!(unsafe { child_leaf.deref().full() });
                        self.split_leaf(
                            result.0,
                            Some(child_key.clone()),
                            child_leaf,
                            &child,
                            guard,
                        )
                    },
                );
            } else if unbounded_leaf == self.leaves.1.load(Relaxed, guard) {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                if unbounded_leaf.is_null() {
                    // unbounded_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(InsertError::Retry((key, value)));
                }
                // Tries to insert into the unbounded leaf, and tries to split the unbounded if it is full.
                return unsafe { unbounded_leaf.deref().insert(key, value, false) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            return Err(InsertError::Duplicated(result.0));
                        }
                        debug_assert!(unsafe { unbounded_leaf.deref().full() });
                        self.split_leaf(result.0, None, unbounded_leaf, &self.leaves.1, guard)
                    },
                );
            }
        }
    }

    pub fn remove<'a>(&'a self, key: &K, guard: &'a Guard) -> Result<bool, RemoveError> {
        loop {
            let unbounded_leaf = (self.leaves.1).load(Relaxed, guard);
            let result = (self.leaves.0).min_greater_equal(&key);
            if let Some((child_key, child)) = result.0 {
                let child_leaf = child.load(Acquire, guard);
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                let (removed, full, obsolete) = unsafe { child_leaf.deref().remove(key, true) };
                if !full {
                    return Ok(removed);
                } else if !obsolete {
                    // Data race resolution.
                    //  - insert: start to insert into a full leaf
                    //  - remove: start removing an entry from the leaf after pointer validation
                    //  - insert: find the leaf full, thus splitting and update
                    //  - remove: find the leaf full, and the leaf node is not locked, returning 'Ok(true)'
                    // Consequently, the key remains.
                    // In order to resolve this, check the pointer again.
                    return self.check_full_leaf(removed, key, child_leaf, guard);
                }
                return self.remove_obsolete_leaf(removed, key, Some(child_key), child_leaf, guard);
            } else if unbounded_leaf == self.leaves.1.load(Acquire, guard) {
                if !(self.leaves.0).validate(result.1) {
                    // Data race resolution: validate metadata - see 'InternalNode::search'.
                    continue;
                }
                if unbounded_leaf.is_null() {
                    // unbounded_leaf being null indicates that the leaf node is bound to be freed.
                    return Err(RemoveError::Retry(false));
                }
                let (removed, full, obsolete) = unsafe { unbounded_leaf.deref().remove(key, true) };
                if !full {
                    return Ok(removed);
                } else if !obsolete {
                    return self.check_full_leaf(removed, key, unbounded_leaf, guard);
                }
                return self.remove_obsolete_leaf(removed, key, None, unbounded_leaf, guard);
            }
        }
    }

    pub fn rollback(&self, guard: &Guard) {
        unsafe {
            self.new_leaves
                .swap(Shared::null(), Release, guard)
                .into_owned()
        };
    }

    pub fn update_link(
        &self,
        anchor_ptr: Shared<LeafNodeAnchor<K, V>>,
        link_ptr: Shared<LeafNode<K, V>>,
    ) {
        // Update order: store(side_link)|release|store(anchor).
        self.side_link.store(link_ptr, Relaxed);
        self.next_node_anchor.store(anchor_ptr, Release);
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
        let mut entry_array: [Option<(Option<&K>, Atomic<Leaf<K, V>>)>; ARRAY_SIZE + 2] = [
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        ];
        let mut num_entries = 0;
        let low_key_leaf_max_key_ref = new_leaves_ref
            .low_key_leaf
            .as_ref()
            .unwrap()
            .max()
            .unwrap()
            .0
            .clone();
        for entry in LeafScanner::new(&self.leaves.0) {
            if new_leaves_ref
                .origin_leaf_key
                .as_ref()
                .map_or_else(|| false, |key| entry.0.cmp(key) == Ordering::Equal)
            {
                if let Some(leaf) = new_leaves_ref.low_key_leaf.take() {
                    entry_array[num_entries]
                        .replace((Some(&low_key_leaf_max_key_ref), Atomic::from(leaf)));
                    num_entries += 1;
                }
                if let Some(leaf) = new_leaves_ref.high_key_leaf.take() {
                    entry_array[num_entries].replace((Some(entry.0), Atomic::from(leaf)));
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
            if let Some(leaf) = new_leaves_ref.low_key_leaf.take() {
                entry_array[num_entries]
                    .replace((Some(&low_key_leaf_max_key_ref), Atomic::from(leaf)));
                num_entries += 1;
            }
            if let Some(leaf) = new_leaves_ref.high_key_leaf.take() {
                entry_array[num_entries].replace((None, Atomic::from(leaf)));
                num_entries += 1;
            }
        }
        debug_assert!(new_leaves_ref.low_key_leaf.is_none());
        debug_assert!(new_leaves_ref.high_key_leaf.is_none());
        debug_assert!(num_entries >= 2);

        let low_key_leaf_array_size = num_entries / 2;
        for (index, entry) in entry_array.iter().enumerate() {
            if let Some(entry) = entry {
                if (index + 1) < low_key_leaf_array_size {
                    low_key_leaves
                        .0
                        .insert(entry.0.unwrap().clone(), entry.1.clone(), false);
                } else if (index + 1) == low_key_leaf_array_size {
                    middle_key.replace(entry.0.unwrap().clone());
                    low_key_leaves
                        .1
                        .store(entry.1.load(Relaxed, guard), Relaxed);
                } else {
                    if let Some(key) = entry.0 {
                        high_key_leaves
                            .0
                            .insert(key.clone(), entry.1.clone(), false);
                    } else {
                        high_key_leaves
                            .1
                            .store(entry.1.load(Relaxed, guard), Relaxed);
                    }
                }
            } else {
                break;
            }
        }
        middle_key
    }

    /// Remediates the broken link state
    pub fn remediate_leaf_node_link(
        &self,
        min_node_anchor: &Atomic<LeafNodeAnchor<K, V>>,
        immediate_smaller_key_leaf_node: Option<&LeafNode<K, V>>,
        low_key_leaf_node: &LeafNode<K, V>,
        high_key_leaf_node: &LeafNode<K, V>,
        guard: &Guard,
    ) {
        // Firstly, replaces the high key node's side link with the new side link.
        // Secondly, links the low key node with the high key node.
        low_key_leaf_node.update_link(
            Shared::null(),
            Atomic::from(high_key_leaf_node as *const _).load(Relaxed, guard),
        );
        high_key_leaf_node.update_link(
            self.next_node_anchor.load(Acquire, guard),
            self.side_link.load(Relaxed, guard),
        );

        // Lastly, links the immediate less key node with the low key node.
        //  - The changes in this step need to be rolled back on operation failure.
        if let Some(node) = immediate_smaller_key_leaf_node {
            node.update_link(
                Shared::null(),
                Atomic::from(low_key_leaf_node as *const _).load(Relaxed, guard),
            );
        } else {
            // The low key node is the minimum node.
            let anchor = min_node_anchor.load(Relaxed, guard);
            if !anchor.is_null() {
                unsafe {
                    anchor.deref().min_leaf_node.store(
                        Atomic::from(low_key_leaf_node as *const _).load(Relaxed, guard),
                        Release,
                    )
                };
            }
        }
    }

    /// Tries to merge two adjacent leaf nodes.
    pub fn try_merge(
        &self,
        prev_leaf_node_key: &K,
        prev_leaf_node: &LeafNode<K, V>,
        prev_prev_leaf_node: Option<&LeafNode<K, V>>,
        guard: &Guard,
    ) -> bool {
        // In order to avoid conflicts with a thread splitting the node, lock itself and prev_leaf_node.
        let self_lock = LeafNodeLocker::lock(self, guard);
        if self_lock.is_none() {
            return false;
        }
        let prev_lock = LeafNodeLocker::lock(prev_leaf_node, guard);
        if prev_lock.is_none() {
            return false;
        }
        debug_assert!(prev_leaf_node.obsolete(false, guard));

        // Inserts the unbounded leaf of the previous leaf node into the leaf array.
        let target_leaf_ref = unsafe { prev_leaf_node.leaves.1.load(Relaxed, guard).deref() };
        if !target_leaf_ref.obsolete() {
            if self
                .leaves
                .0
                .insert(
                    prev_leaf_node_key.clone(),
                    prev_leaf_node.leaves.1.clone(),
                    false,
                )
                .is_some()
            {
                return false;
            }
        }
        if let Some(prev_prev_leaf_node) = prev_prev_leaf_node {
            // Updates the side link of the prev-prev leaf node.
            let next_leaf_node = prev_leaf_node.side_link.load(Relaxed, guard);
            debug_assert_eq!(next_leaf_node.as_raw(), self as *const _);
            // Copies the link to the low smaller key leaf node.
            prev_prev_leaf_node.update_link(Shared::null(), next_leaf_node);
        }
        prev_leaf_node.leaves.1.store(Shared::null(), Relaxed);
        debug_assert!(prev_leaf_node.obsolete(true, guard));
        true
    }

    /// Splits a full leaf.
    fn split_leaf(
        &self,
        entry: (K, V),
        full_leaf_key: Option<K>,
        full_leaf_shared: Shared<Leaf<K, V>>,
        full_leaf_ptr: &Atomic<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<(), InsertError<K, V>> {
        let mut new_leaves_ptr;
        match self.new_leaves.compare_and_set(
            Shared::null(),
            Owned::new(NewLeaves {
                origin_leaf_key: full_leaf_key,
                origin_leaf_ptr: full_leaf_ptr.clone(),
                low_key_leaf: None,
                high_key_leaf: None,
            }),
            Acquire,
            guard,
        ) {
            Ok(result) => new_leaves_ptr = result,
            Err(_) => return Err(InsertError::Retry(entry)),
        }

        // Checks the full leaf pointer and the leaf node state after locking the leaf node.
        if full_leaf_shared != full_leaf_ptr.load(Relaxed, guard) {
            let obsolete_leaf = self.new_leaves.swap(Shared::null(), Relaxed, guard);
            drop(unsafe { obsolete_leaf.into_owned() });
            return Err(InsertError::Retry(entry));
        }
        debug_assert!(unsafe { full_leaf_shared.deref().full() });

        // Copies entries to the newly allocated leaves.
        let new_leaves_ref = unsafe { new_leaves_ptr.deref_mut() };
        unsafe {
            full_leaf_shared.deref().distribute(
                &mut new_leaves_ref.low_key_leaf,
                &mut new_leaves_ref.high_key_leaf,
            )
        };

        // Inserts the given entry.
        if new_leaves_ref.low_key_leaf.is_none() {
            new_leaves_ref.low_key_leaf.replace(Box::new(Leaf::new()));
            new_leaves_ref.low_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        } else if new_leaves_ref.high_key_leaf.is_none()
            || new_leaves_ref
                .low_key_leaf
                .as_ref()
                .unwrap()
                .min_greater_equal(&entry.0)
                .0
                .is_some()
        {
            // Inserts the entry into the low-key leaf if the high-key leaf is empty, or the key fits the low-key leaf.
            new_leaves_ref.low_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        } else {
            // Inserts the entry into the high-key leaf.
            new_leaves_ref.high_key_leaf.as_ref().unwrap().insert(
                entry.0.clone(),
                entry.1.clone(),
                false,
            );
        }

        // Inserts the newly added leaves into the main array.
        let unused_leaf;
        if new_leaves_ref.high_key_leaf.is_none() {
            // Replaces the full leaf with the low-key leaf.
            unused_leaf = full_leaf_ptr.swap(
                Owned::from(new_leaves_ref.low_key_leaf.take().unwrap()),
                Release,
                &guard,
            );
        } else {
            let max_key = new_leaves_ref
                .low_key_leaf
                .as_ref()
                .unwrap()
                .max()
                .unwrap()
                .0;
            if let Some(leaf) = self.leaves.0.insert(
                max_key.clone(),
                Atomic::from(new_leaves_ref.low_key_leaf.take().unwrap()),
                false,
            ) {
                // Insertion failed: expect that the parent splits the leaf node.
                new_leaves_ref
                    .low_key_leaf
                    .replace(unsafe { (leaf.0).1.into_owned().into_box() });
                return Err(InsertError::Full(entry));
            }

            // Replaces the full leaf with the high-key leaf.
            unused_leaf = full_leaf_ptr.swap(
                Owned::from(new_leaves_ref.high_key_leaf.take().unwrap()),
                Release,
                &guard,
            );
        }

        // Drops the deprecated leaves.
        let unused_leaves = self.new_leaves.swap(Shared::null(), Release, guard);
        unsafe {
            let new_split_leaves = unused_leaves.into_owned();
            debug_assert_eq!(
                new_split_leaves.origin_leaf_ptr.load(Relaxed, guard),
                unused_leaf
            );
            debug_assert!(unused_leaf.deref().full());
            guard.defer_destroy(unused_leaf);
        };

        // OK
        Ok(())
    }

    fn check_full_leaf(
        &self,
        removed: bool,
        key: &K,
        leaf_shared: Shared<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<bool, RemoveError> {
        // There is a chance that the target key value pair has been copied to new_leaves.
        //  - Remove: release(leaf)|load(mutex)|acquire|load(leaf_ptr)
        //  - Insert: load(leaf)|store(mutex)|acquire|release|store(leaf_ptr)|release|store(mutex)
        // The remove thread either reads the locked mutex state or the updated leaf pointer value.
        if !self.new_leaves.load(Acquire, guard).is_null() {
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

    /// Removes the obsolete leaf.
    fn remove_obsolete_leaf(
        &self,
        removed: bool,
        key: &K,
        child_key: Option<&K>,
        leaf_shared: Shared<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<bool, RemoveError> {
        // If locked and the pointer has remained the same, invalidate the leaf.
        let lock = LeafNodeLocker::lock(self, guard);
        if lock.is_none() {
            return Err(RemoveError::Retry(removed));
        }

        let result = (self.leaves.0).min_greater_equal(&key);
        let leaf_ptr = if let Some((_, child)) = result.0 {
            &child
        } else {
            &self.leaves.1
        };
        if leaf_ptr.load(Relaxed, guard) != leaf_shared {
            return Err(RemoveError::Retry(removed));
        }

        // The last remaining leaf is the unbounded one, return RemoveError::Coalesce.
        let coalesce = child_key.map_or_else(
            || self.leaves.0.obsolete(),
            |key| {
                let obsolete = self.leaves.0.remove(key, true).2;
                // Once the key is removed, it is safe to drop the leaf as the validation loop ensures the absence of readers.
                leaf_ptr.store(Shared::null(), Release);
                unsafe { guard.defer_destroy(leaf_shared) };
                obsolete
            },
        );

        if coalesce {
            Err(RemoveError::Coalesce(removed))
        } else {
            Ok(removed)
        }
    }

    fn next<'a>(&self, guard: &'a Guard) -> Option<&'a LeafNode<K, V>> {
        // Read order: load(anchor)|acquire|load(side_link).
        let next_node_anchor = self.next_node_anchor.load(Acquire, guard);
        let next_leaf_node = if !next_node_anchor.is_null() {
            unsafe { next_node_anchor.deref().next_leaf_node(guard) }
        } else {
            self.side_link.load(Relaxed, guard)
        };
        if !next_leaf_node.is_null() {
            return Some(unsafe { next_leaf_node.deref() });
        }
        None
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
        let unbounded_leaf_ptr = self.leaves.1.load(Relaxed, &guard);
        if !unbounded_leaf_ptr.is_null() {
            let leaf_ref = unsafe { unbounded_leaf_ptr.deref() };
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
                    let font_color = if !leaf_scanner.removed() {
                        "black"
                    } else {
                        "red"
                    };
                    output.write_fmt(format_args!(
                        "<tr><td><font color=\"{}\">{}</font></td><td>{}</td><td>{}</td></tr>\n",
                        font_color, rank, entry.0, entry.1,
                    ))?;
                    rank += 1;
                }
                output.write_fmt(format_args!("</table>\n>]\n"))?;
            }
        }

        // Prints the anchor and side link.
        let leaf_node_anchor = self.next_node_anchor.load(Relaxed, guard);
        if !leaf_node_anchor.is_null() {
            output.write_fmt(format_args!(
                "{} -> {}\n",
                self.id(),
                unsafe { leaf_node_anchor.deref() }.id()
            ))?;
        }
        let side_link = self.side_link.load(Relaxed, guard);
        if !side_link.is_null() {
            output.write_fmt(format_args!(
                "{} -> {}\n",
                self.id(),
                unsafe { side_link.deref() }.id()
            ))?;
        }

        std::io::Result::Ok(())
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for LeafNode<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        // Drops entries related to the unused child.
        let unused_leaves = self.new_leaves.load(Acquire, &guard);
        if !unused_leaves.is_null() {
            // Drops only the origin node, assuming that the rest are copied.
            let unused_leaves = unsafe { unused_leaves.into_owned() };
            let obsolete_leaf = unused_leaves.origin_leaf_ptr.load(Relaxed, &guard);
            if !obsolete_leaf.is_null() {
                drop(unsafe { obsolete_leaf.into_owned() });
            }
        } else {
            // Drops all.
            for entry in LeafScanner::new(&self.leaves.0) {
                let child = entry.1.load(Acquire, &guard);
                if !child.is_null() {
                    drop(unsafe { child.into_owned() });
                }
            }
            let unbounded_leaf = self.leaves.1.load(Acquire, &guard);
            if !unbounded_leaf.is_null() {
                drop(unsafe { unbounded_leaf.into_owned() });
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
    pub fn lock(leaf_node: &'a LeafNode<K, V>, guard: &Guard) -> Option<LeafNodeLocker<'a, K, V>> {
        let mut new_nodes_dummy = NewLeaves::new();
        if let Err(error) = leaf_node.new_leaves.compare_and_set(
            Shared::null(),
            unsafe { Owned::from_raw(&mut new_nodes_dummy as *mut NewLeaves<K, V>) },
            Acquire,
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

/// Leaf node scanner.
pub struct LeafNodeScanner<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    min_allowed_key: Option<&'a K>,
    leaf_node: &'a LeafNode<K, V>,
    leaf_pointer_array: [Option<Shared<'a, Leaf<K, V>>>; ARRAY_SIZE + 1],
    current_leaf_index: usize,
    leaf_scanner: Option<LeafScanner<'a, K, V>>,
    guard: &'a Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNodeScanner<'a, K, V> {
    fn new(
        min_allowed_key: Option<&'a K>,
        leaf_node: &'a LeafNode<K, V>,
        guard: &'a Guard,
    ) -> LeafNodeScanner<'a, K, V> {
        LeafNodeScanner::<'a, K, V> {
            min_allowed_key,
            leaf_node,
            leaf_pointer_array: [None; ARRAY_SIZE + 1],
            current_leaf_index: 0,
            leaf_scanner: None,
            guard,
        }
    }

    fn from(
        min_allowed_key: &K,
        leaf_node: &'a LeafNode<K, V>,
        guard: &'a Guard,
    ) -> Option<LeafNodeScanner<'a, K, V>> {
        let mut leaf_node_scanner = LeafNodeScanner::<'a, K, V> {
            min_allowed_key: None,
            leaf_node,
            leaf_pointer_array: [None; ARRAY_SIZE + 1],
            current_leaf_index: 0,
            leaf_scanner: None,
            guard,
        };
        leaf_node_scanner.start();
        while let Some(leaf_ptr) =
            leaf_node_scanner.leaf_pointer_array[leaf_node_scanner.current_leaf_index].take()
        {
            leaf_node_scanner.current_leaf_index += 1;
            let leaf_scanner =
                LeafScanner::from(unsafe { leaf_ptr.deref() }, min_allowed_key, false);
            if let Some(leaf_scanner) = leaf_scanner {
                leaf_node_scanner.leaf_scanner.replace(leaf_scanner);
                return Some(leaf_node_scanner);
            }
            if leaf_node_scanner.current_leaf_index == leaf_node_scanner.leaf_pointer_array.len() {
                break;
            }
        }

        // Returns the first valid leaf node scanner.
        let mut jump_scanner = leaf_node_scanner.jump(None, guard);
        while let Some(mut scanner) = jump_scanner.take() {
            if scanner.next().is_some() {
                return Some(scanner);
            }
            jump_scanner = scanner.jump(None, guard);
        }

        None
    }

    fn start(&mut self) {
        // Starts scanning.
        loop {
            // Data race resolution: validate metadata - see 'InternalNode::search'.
            let mut index = 0;
            let mut scanner = LeafScanner::new(&self.leaf_node.leaves.0);
            while let Some(entry) = scanner.next() {
                self.leaf_pointer_array[index].replace(entry.1.load(Relaxed, &self.guard));
                index += 1;
            }
            let ptr = self.leaf_node.leaves.1.load(Relaxed, &self.guard);
            if !ptr.is_null() {
                self.leaf_pointer_array[index].replace(ptr);
                index += 1;
            }
            if self.leaf_node.leaves.0.validate(scanner.metadata()) {
                break;
            }
            for i in 0..index {
                self.leaf_pointer_array[i].take();
            }
        }
    }

    pub fn jump(
        &self,
        min_allowed_key: Option<&'a K>,
        guard: &'a Guard,
    ) -> Option<LeafNodeScanner<'a, K, V>> {
        if let Some(next_node) = self.leaf_node.next(guard) {
            Some(LeafNodeScanner::new(min_allowed_key, next_node, guard))
        } else {
            None
        }
    }

    /// Returns a reference to the entry that the scanner is currently pointing to
    pub fn get(&self) -> Option<(&'a K, &'a V)> {
        if let Some(leaf_scanner) = self.leaf_scanner.as_ref() {
            return leaf_scanner.get();
        }
        None
    }
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Iterator
    for LeafNodeScanner<'a, K, V>
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let min_allowed_key = self.leaf_scanner.as_ref().map_or_else(
            || {
                self.min_allowed_key
                    .as_ref()
                    .map_or_else(|| None, |key| Some(*key))
            },
            |scanner| scanner.get().map_or_else(|| None, |(key, _)| Some(key)),
        );
        if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
            // Leaf iteration.
            if let Some(entry) = leaf_scanner.next() {
                return Some(entry);
            }
            if self.current_leaf_index == usize::MAX {
                return None;
            }
        }

        if self.current_leaf_index == 0 && self.leaf_scanner.is_none() {
            self.start();
        }

        if self.current_leaf_index < self.leaf_pointer_array.len() {
            // Proceeds to the next leaf.
            while let Some(leaf_ptr) = self.leaf_pointer_array[self.current_leaf_index].take() {
                self.leaf_scanner
                    .replace(LeafScanner::new(unsafe { leaf_ptr.deref() }));
                self.current_leaf_index += 1;
                if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                    // Leaf iteration.
                    for entry in leaf_scanner {
                        // Data race resolution: compare keys.
                        //  - Writer: insert an intermediate low key leaf
                        //  - Reader: read the low key leaf pointer
                        //  - Reader: read the old full leaf pointer
                        //  - Writer: replace the old full leaf pointer with a new one
                        // Consequently, the scanner reads outdated smaller values.
                        if min_allowed_key
                            .map_or_else(|| true, |key| key.cmp(entry.0) == Ordering::Less)
                        {
                            return Some(entry);
                        }
                    }
                }
                self.leaf_scanner.take();
                if self.current_leaf_index == self.leaf_pointer_array.len() {
                    break;
                }
            }
        }
        self.current_leaf_index = usize::MAX;
        None
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
    low_key_leaf: Option<Box<Leaf<K, V>>>,
    high_key_leaf: Option<Box<Leaf<K, V>>>,
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
            low_key_leaf: None,
            high_key_leaf: None,
        }
    }
}

/// Minimum leaf node anchor for Scanner.
///
/// An instance of LeafNodeAnchor is deallocated by Scanner, or adjacent node cleanup.
pub struct LeafNodeAnchor<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Next node anchor pointer.
    ///
    /// It is only set when the owner node is being deprecated.
    next_valid_node_anchor: Atomic<LeafNodeAnchor<K, V>>,
    /// Minimum leaf node pointer.
    min_leaf_node: Atomic<LeafNode<K, V>>,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNodeAnchor<K, V> {
    pub fn new() -> LeafNodeAnchor<K, V> {
        LeafNodeAnchor {
            next_valid_node_anchor: Atomic::null(),
            min_leaf_node: Atomic::null(),
        }
    }

    /// Takes the memory address of the instance as an identifier.
    pub fn id(&self) -> usize {
        self as *const _ as usize
    }

    pub fn set(&self, ptr: Atomic<LeafNode<K, V>>, guard: &Guard) {
        self.min_leaf_node.store(ptr.load(Relaxed, guard), Release);
    }

    pub fn deprecate(&self, ptr: Shared<LeafNodeAnchor<K, V>>, guard: &Guard) {
        debug_assert!(self.next_valid_node_anchor.load(Relaxed, guard).is_null());
        // update order: store(next_ptr)|release|store(min_ptr)
        self.next_valid_node_anchor.swap(ptr, Relaxed, guard);
        self.min_leaf_node.store(Shared::null(), Release);
    }

    pub fn min_leaf_node<'a>(&self, guard: &'a Guard) -> Shared<'a, LeafNode<K, V>> {
        self.min_leaf_node.load(Relaxed, guard)
    }

    pub fn next_leaf_node<'a>(&self, guard: &'a Guard) -> Shared<'a, LeafNode<K, V>> {
        let mut next_node_anchor = self.next_valid_node_anchor.load(Acquire, &guard);
        while !next_node_anchor.is_null() {
            let next_node_anchor_ref = unsafe { next_node_anchor.deref() };
            let next_min_leaf_node = next_node_anchor_ref.min_leaf_node.load(Acquire, guard);
            let next_next_node_anchor = next_node_anchor_ref
                .next_valid_node_anchor
                .load(Relaxed, &guard);
            if !next_next_node_anchor.is_null() {
                // TODO #20: when to garbage collect anchors?
                next_node_anchor = next_next_node_anchor;
            } else {
                return next_min_leaf_node;
            }
        }
        self.min_leaf_node(guard)
    }

    pub fn next_valid_node_anchor<'a>(&self, guard: &'a Guard) -> Shared<'a, LeafNodeAnchor<K, V>> {
        self.next_valid_node_anchor.load(Relaxed, guard)
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for LeafNodeAnchor<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let mut next_node_anchor = self.next_valid_node_anchor.load(Acquire, &guard);
        while !next_node_anchor.is_null() {
            let next_node_anchor_ref = unsafe { next_node_anchor.deref() };
            let next_next_node_anchor = next_node_anchor_ref
                .next_valid_node_anchor
                .load(Acquire, &guard);
            if !next_next_node_anchor.is_null() {
                // It is safe to drop next_node_anchor immediately as it is only reachable from self.
                drop(unsafe { next_node_anchor.into_owned() });
                next_node_anchor = next_next_node_anchor;
            } else {
                break;
            }
        }
    }
}
