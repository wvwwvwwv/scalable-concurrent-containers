use super::leaf::{InsertResult, Leaf, RemoveResult, Scanner, DIMENSION};
use super::leaf_node::{LOCKED, RETIRED};
use super::node::{Node, Type};

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};

use std::borrow::Borrow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};

/// Internal node.
///
/// The layout of an internal node: |ptr(children)/max(child keys)|...|ptr(children)|
pub struct InternalNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Children of the [`InternalNode`].
    children: Leaf<K, AtomicArc<Node<K, V>>>,

    /// A child [`Node`] that has no upper key bound.
    ///
    /// It stores the maximum key in the node, and key-value pairs are firstly pushed to this
    /// [`Node`].
    unbounded_child: AtomicArc<Node<K, V>>,

    /// `latch` acts as a mutex of the [`InternalNode`] that also stores the information about an
    /// on-going structural change.
    latch: AtomicArc<StructuralChange<K, V>>,
}

impl<K, V> InternalNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new empty internal node.
    pub fn new() -> InternalNode<K, V> {
        InternalNode {
            children: Leaf::new(),
            unbounded_child: AtomicArc::null(),
            latch: AtomicArc::null(),
        }
    }

    /// Returns the depth of the node.
    pub fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            return unbounded_ref.depth(depth + 1, barrier);
        }
        depth
    }

    /// Returns `true` if the [`InternalNode`] has retired.
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
                        // Data race resolution - see `LeafNode::search`.
                        return child.search(key, barrier);
                    }
                }
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if !self.children.validate(metadata) {
                    continue;
                }
                return unbounded.search(key, barrier);
            }
            return None;
        }
    }

    /// Returns the minimum key entry.
    pub fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
        loop {
            let mut retry = false;
            let scanner = Scanner::new(&self.children);
            let metadata = scanner.metadata();
            for (_, child) in scanner {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        if let Some(scanner) = child.min(barrier) {
                            return Some(scanner);
                        }
                        continue;
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                retry = true;
                break;
            }
            if retry {
                continue;
            }
            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if self.children.validate(metadata) {
                    return unbounded.min(barrier);
                }
                continue;
            }
            return None;
        }
    }

    /// Returns a [`Scanner`] pointing to an entry that is close enough to the entry with the
    /// maximum key among those keys smaller than the given key.
    ///
    /// It returns `None` if all the keys in the [`InternalNode`] is equal to or greater than the
    /// given key.
    pub fn max_less_appr<'b, Q>(&self, key: &Q, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>>
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
                            if let Some(scanner) = child.max_less_appr(key, barrier) {
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
                if k.borrow() < key {
                    return Some(min_scanner);
                }
                break;
            }
            min_scanner = min_scanner.jump(None, barrier)?;
        }

        None
    }

    /// Inserts a key-value pair.
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
                if let Some(child_ref) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        match child_ref.insert(key, value, barrier)? {
                            InsertResult::Success => return Ok(InsertResult::Success),
                            InsertResult::Duplicate(key, value) => {
                                return Ok(InsertResult::Duplicate(key, value));
                            }
                            InsertResult::Full(key, value) | InsertResult::Retired(key, value) => {
                                return self.split_node(
                                    key,
                                    value,
                                    Some(child_key),
                                    child_ptr,
                                    child,
                                    false,
                                    barrier,
                                );
                            }
                        };
                    }
                }
                // It is not a hot loop - see `LeafNode::search`.
                continue;
            }

            let unbounded_ptr = self.unbounded_child.load(Acquire, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                debug_assert!(unbounded_ptr.tag() == Tag::None);
                if !self.children.validate(metadata) {
                    continue;
                }
                match unbounded.insert(key, value, barrier)? {
                    InsertResult::Success => return Ok(InsertResult::Success),
                    InsertResult::Duplicate(key, value) => {
                        return Ok(InsertResult::Duplicate(key, value));
                    }
                    InsertResult::Full(key, value) | InsertResult::Retired(key, value) => {
                        return self.split_node(
                            key,
                            value,
                            None,
                            unbounded_ptr,
                            &self.unbounded_child,
                            false,
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
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let result = child.remove_if(key, condition, barrier)?;
                        if result == RemoveResult::Retired {
                            return self.coalesce(barrier);
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
                let result = unbounded.remove_if(key, condition, barrier)?;
                if result == RemoveResult::Retired {
                    return self.coalesce(barrier);
                }
                return Ok(result);
            }
            return Err(false);
        }
    }

    /// Splits a full node.
    ///
    /// # Errors
    ///
    /// Returns an error if retry is required.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    fn split_node(
        &self,
        key: K,
        value: V,
        full_node_key: Option<&K>,
        full_node_ptr: Ptr<Node<K, V>>,
        full_node: &AtomicArc<Node<K, V>>,
        root_node_split: bool,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        let new_nodes = match self.latch.compare_exchange(
            Ptr::null(),
            (
                Some(Arc::new(StructuralChange {
                    origin_node_key: None,
                    origin_node: full_node.clone(Relaxed, barrier),
                    low_key_node: AtomicArc::null(),
                    middle_key: None,
                    high_key_node: AtomicArc::null(),
                })),
                Tag::None,
            ),
            Acquire,
            Relaxed,
        ) {
            Ok((_, ptr)) => {
                if self.retired(Relaxed) || full_node_ptr != full_node.load(Relaxed, barrier) {
                    // `self` is now obsolete, or `full_leaf` has changed in the meantime.
                    drop(self.latch.swap((None, Tag::None), Relaxed));
                    //full_node_ref.rollback(barrier);
                    return Err((key, value));
                }
                unsafe { &mut *(ptr.as_raw() as *mut StructuralChange<K, V>) }
            }
            Err(_) => {
                //full_node_ref.rollback(barrier);
                return Err((key, value));
            }
        };

        if let Some(full_node_key) = full_node_key {
            let ptr = &new_nodes.origin_node_key as *const Option<K> as *mut Option<K>;
            unsafe {
                ptr.write(Some(full_node_key.clone()));
            }
        }

        let target = full_node_ptr.as_ref().unwrap();

        match target.node() {
            Type::Internal(full_internal_node) => {
                let new_children = full_internal_node
                    .latch
                    .load(Relaxed, barrier)
                    .as_ref()
                    .unwrap();

                // Copies nodes except for the known full node to the newly allocated internal node entries.
                let internal_nodes = (
                    Arc::new(Node::new_internal_node()),
                    Arc::new(Node::new_internal_node()),
                );
                let low_key_nodes =
                    if let Type::Internal(low_key_internal_node) = &internal_nodes.0.node() {
                        Some(low_key_internal_node)
                    } else {
                        None
                    };
                let high_key_nodes =
                    if let Type::Internal(high_key_internal_node) = &internal_nodes.1.node() {
                        Some(high_key_internal_node)
                    } else {
                        None
                    };

                // Builds a list of valid nodes.
                #[allow(clippy::type_complexity)]
                let mut entry_array: [Option<(Option<&K>, AtomicArc<Node<K, V>>)>;
                    DIMENSION.num_entries + 2] = Default::default();
                let mut num_entries = 0;
                for entry in Scanner::new(&full_internal_node.children) {
                    if new_children
                        .origin_node_key
                        .as_ref()
                        .map_or_else(|| false, |key| entry.0.borrow() == key)
                    {
                        let low_key_node_shared = new_children.low_key_node.load(Relaxed, barrier);
                        if !low_key_node_shared.is_null() {
                            entry_array[num_entries].replace((
                                Some(new_children.middle_key.as_ref().unwrap()),
                                new_children.low_key_node.clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                        let high_key_node_shared =
                            new_children.high_key_node.load(Relaxed, barrier);
                        if !high_key_node_shared.is_null() {
                            entry_array[num_entries].replace((
                                Some(entry.0),
                                new_children.high_key_node.clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                    } else {
                        entry_array[num_entries]
                            .replace((Some(entry.0), entry.1.clone(Relaxed, barrier)));
                        num_entries += 1;
                    }
                }
                if new_children.origin_node_key.is_some() {
                    // If the origin is a bounded node, assign the unbounded node to the high key
                    // node's unbounded.
                    entry_array[num_entries].replace((
                        None,
                        full_internal_node.unbounded_child.clone(Relaxed, barrier),
                    ));
                    num_entries += 1;
                } else {
                    // If the origin is an unbounded node, assign the high key node to the high key
                    // node's unbounded.
                    let low_key_node_shared = new_children.low_key_node.load(Relaxed, barrier);
                    if !low_key_node_shared.is_null() {
                        entry_array[num_entries].replace((
                            Some(new_children.middle_key.as_ref().unwrap()),
                            new_children.low_key_node.clone(Relaxed, barrier),
                        ));
                        num_entries += 1;
                    }
                    let high_key_node_shared = new_children.high_key_node.load(Relaxed, barrier);
                    if !high_key_node_shared.is_null() {
                        entry_array[num_entries]
                            .replace((None, new_children.high_key_node.clone(Relaxed, barrier)));
                        num_entries += 1;
                    }
                }
                debug_assert!(num_entries >= 2);

                let low_key_node_array_size = num_entries / 2;
                for (index, entry) in entry_array.iter().enumerate() {
                    if let Some(entry) = entry {
                        match (index + 1).cmp(&low_key_node_array_size) {
                            Less => {
                                low_key_nodes.unwrap().children.insert(
                                    entry.0.unwrap().clone(),
                                    entry.1.clone(Relaxed, barrier),
                                );
                            }
                            Equal => {
                                new_nodes.middle_key.replace(entry.0.unwrap().clone());
                                low_key_nodes
                                    .unwrap()
                                    .unbounded_child
                                    .swap((entry.1.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                            }
                            Greater => {
                                if let Some(key) = entry.0 {
                                    high_key_nodes
                                        .unwrap()
                                        .children
                                        .insert(key.clone(), entry.1.clone(Relaxed, barrier));
                                } else {
                                    high_key_nodes.as_ref().unwrap().unbounded_child.swap(
                                        (entry.1.get_arc(Relaxed, barrier), Tag::None),
                                        Relaxed,
                                    );
                                }
                            }
                        };
                    } else {
                        break;
                    }
                }

                // Turns the new nodes into internal nodes.
                new_nodes
                    .low_key_node
                    .swap((Some(internal_nodes.0), Tag::None), Relaxed);
                new_nodes
                    .high_key_node
                    .swap((Some(internal_nodes.1), Tag::None), Relaxed);
            }
            Type::Leaf(full_leaf_node) => {
                // Copies leaves except for the known full leaf to the newly allocated leaf node entries.
                let leaf_nodes = (
                    Arc::new(Node::new_leaf_node()),
                    Arc::new(Node::new_leaf_node()),
                );
                let low_key_leaf_node = if let Type::Leaf(low_key_leaf_node) = &leaf_nodes.0.node()
                {
                    Some(low_key_leaf_node)
                } else {
                    None
                };
                let high_key_leaf_node =
                    if let Type::Leaf(high_key_leaf_node) = &leaf_nodes.1.node() {
                        Some(high_key_leaf_node)
                    } else {
                        None
                    };
                full_leaf_node
                    .split_leaf_node(
                        low_key_leaf_node.unwrap(),
                        high_key_leaf_node.unwrap(),
                        barrier,
                    )
                    .map(|middle_key| new_nodes.middle_key.replace(middle_key));

                // Turns the new leaves into leaf nodes.
                new_nodes
                    .low_key_node
                    .swap((Some(leaf_nodes.0), Tag::None), Relaxed);
                new_nodes
                    .high_key_node
                    .swap((Some(leaf_nodes.1), Tag::None), Relaxed);
            }
        };

        // The full node is the current root: split_root processes the rest.
        if root_node_split {
            return Ok(InsertResult::Full(key, value));
        }

        // Inserts the newly allocated internal nodes into the main array.
        if let InsertResult::Full(middle_key, _) = self.children.insert(
            new_nodes.middle_key.take().unwrap(),
            new_nodes.low_key_node.clone(Relaxed, barrier),
        ) {
            // Insertion failed: expects that the parent splits this node.
            new_nodes.middle_key.replace(middle_key);
            return Ok(InsertResult::Full(key, value));
        }

        // Replaces the full node with the high-key node.
        let unused_node = full_node.swap(
            (new_nodes.high_key_node.get_arc(Relaxed, barrier), Tag::None),
            Release,
        );

        // Drops the deprecated nodes.
        // - Still, the deprecated full leaf can be reachable by Scanners.
        if let Some(unused_node) = unused_node {
            // Cleans up the split operation by committing it.
            unused_node.commit(barrier);
            barrier.reclaim(unused_node);
        }

        // Unlocks the node.
        if let Some(change) = self.latch.swap((None, Tag::None), Release) {
            barrier.reclaim(change);
        }

        // Since a new node has been inserted, the caller can retry.
        Err((key, value))
    }

    /// Commits an on-going structural change.
    pub fn commit(&self, barrier: &Barrier) {
        // Keeps the internal node locked to prevent further locking attempts.
        if let Some(change) = self.latch.swap((None, LOCKED), Relaxed) {
            let obsolete_node_ptr = change.origin_node.load(Relaxed, barrier);
            if let Some(obsolete_node_ref) = obsolete_node_ptr.as_ref() {
                obsolete_node_ref.commit(barrier);
            };
        }
    }

    /// Rolls back the ongoing split operation recursively.
    pub fn rollback(&self, barrier: &Barrier) {
        if let Some(change) = self.latch.load(Relaxed, barrier).as_ref() {
            if let Some(origin) = change.origin_node.swap((None, Tag::None), Relaxed) {
                origin.rollback(barrier);
            }
        }

        // Unlocks the node after the origin node has been cleaned up.
        if let Some(change) = self.latch.swap((None, Tag::None), Release) {
            barrier.reclaim(change);
        }
    }

    /// Tries to coalesce nodes.
    fn coalesce(&self, barrier: &Barrier) -> Result<RemoveResult, bool> {
        let lock = Locker::try_lock(self);
        if lock.is_none() {
            return Err(true);
        }

        let mut num_valid_leaves = 0;
        for entry in Scanner::new(&self.children) {
            let node_ptr = entry.1.load(Relaxed, barrier);
            let node_ref = node_ptr.as_ref().unwrap();
            if node_ref.retired(Relaxed) {
                let result = self.children.remove_if(entry.0, &mut |_| true);
                debug_assert_ne!(result, RemoveResult::Fail);

                // Once the key is removed, it is safe to deallocate the node as the validation
                // loop ensures the absence of readers.
                if let Some(node) = entry.1.swap((None, Tag::None), Release) {
                    barrier.reclaim(node);
                }
            } else {
                num_valid_leaves += 1;
            }
        }

        // The unbounded leaf becomes unreachable after all the other leaves are gone.
        let fully_empty = if num_valid_leaves == 0 {
            let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.retired(Relaxed) {
                    if let Some(obsolete_node) = self.unbounded_child.swap((None, RETIRED), Release)
                    {
                        barrier.reclaim(obsolete_node);
                    }
                    true
                } else {
                    false
                }
            } else {
                debug_assert!(unbounded_ptr.tag() == RETIRED);
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
}

/// [`Locker`] holds exclusive access to a [`Leaf`].
struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    lock: &'n AtomicArc<StructuralChange<K, V>>,
    deprecate: bool,
}

impl<'n, K, V> Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn try_lock(internal_node: &'n InternalNode<K, V>) -> Option<Locker<'n, K, V>> {
        if internal_node
            .latch
            .compare_exchange(Ptr::null(), (None, LOCKED), Acquire, Relaxed)
            .is_ok()
        {
            Some(Locker {
                lock: &internal_node.latch,
                deprecate: false,
            })
        } else {
            None
        }
    }

    fn deprecate(&mut self) {
        self.deprecate = true;
    }
}

impl<'n, K, V> Drop for Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn drop(&mut self) {
        if !self.deprecate {
            self.lock.swap((None, Tag::None), Release);
        }
    }
}

/// [`StructuralChange`] stores intermediate results during a split/merge operation.
struct StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    origin_node_key: Option<K>,
    origin_node: AtomicArc<Node<K, V>>,
    low_key_node: AtomicArc<Node<K, V>>,
    middle_key: Option<K>,
    high_key_node: AtomicArc<Node<K, V>>,
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use proptest::prelude::*;
    use tokio::sync;

    #[test]
    fn basic() {}

    proptest! {
        #[test]
        fn prop(_insert in 0_usize..4096) {}
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 16_usize;
        let workload_size = 64_usize;
        for _ in 0..8 {
            for _ in 0..=workload_size {
                let barrier = Arc::new(sync::Barrier::new(num_tasks));
                let internal_node: Arc<InternalNode<usize, usize>> = Arc::new(InternalNode::new());
                let inserted: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
                let mut task_handles = Vec::with_capacity(num_tasks);
                for _ in 0..num_tasks {
                    let barrier_clone = barrier.clone();
                    let internal_node_clone = internal_node.clone();
                    let inserted_clone = inserted.clone();
                    task_handles.push(tokio::spawn(async move {
                        barrier_clone.wait().await;
                        drop(internal_node_clone);
                        inserted_clone.swap(true, Relaxed);
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
