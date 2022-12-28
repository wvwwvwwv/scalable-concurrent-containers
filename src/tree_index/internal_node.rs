use super::leaf::{InsertResult, Leaf, RemoveResult, Scanner, DIMENSION};
use super::leaf_node::RETIRED;
use super::node::{Node, Type};

use crate::ebr::{Arc, AtomicArc, Barrier, Ptr, Tag};
use crate::wait_queue::{DeriveAsyncWait, WaitQueue};

use std::borrow::Borrow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::ptr;
use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU8};

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
    pub(super) unbounded_child: AtomicArc<Node<K, V>>,

    /// On-going split operation.
    split_op: StructuralChange<K, V>,

    /// Latch of the [`InternalNode`].
    latch: AtomicU8,

    /// `wait_queue` for `latch`.
    wait_queue: WaitQueue,
}

impl<K, V> InternalNode<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    /// Creates a new empty internal node.
    #[inline]
    pub(super) fn new() -> InternalNode<K, V> {
        InternalNode {
            children: Leaf::new(),
            unbounded_child: AtomicArc::null(),
            split_op: StructuralChange::default(),
            latch: AtomicU8::new(0),
            wait_queue: WaitQueue::default(),
        }
    }

    /// Returns the depth of the node.
    #[inline]
    pub(super) fn depth(&self, depth: usize, barrier: &Barrier) -> usize {
        let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
        if let Some(unbounded_ref) = unbounded_ptr.as_ref() {
            return unbounded_ref.depth(depth + 1, barrier);
        }
        depth
    }

    /// Returns `true` if the [`InternalNode`] has retired.
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
    #[inline]
    pub(super) fn min<'b>(&self, barrier: &'b Barrier) -> Option<Scanner<'b, K, V>> {
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
    /// maximum key among those keys smaller than or equal to the given key.
    ///
    /// It returns `None` if all the keys in the [`InternalNode`] is equal to or greater than the
    /// given key.
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
                            if let Some(scanner) = child.max_le_appr(key, barrier) {
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
                        let insert_result = child_ref.insert(key, value, async_wait, barrier)?;
                        match insert_result {
                            InsertResult::Success
                            | InsertResult::Duplicate(..)
                            | InsertResult::Frozen(..) => return Ok(insert_result),
                            InsertResult::Full(k, v) => {
                                let split_result = self.split_node(
                                    k,
                                    v,
                                    Some(child_key),
                                    child_ptr,
                                    child,
                                    false,
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
                            InsertResult::Retired(k, v) => {
                                debug_assert!(child_ref.retired(Relaxed));
                                if self.coalesce(barrier) == RemoveResult::Retired {
                                    debug_assert!(self.retired(Relaxed));
                                    return Ok(InsertResult::Retired(k, v));
                                }
                                return Err((k, v));
                            }
                            InsertResult::Retry(k, v) => {
                                // `child` has been split, therefore it can be retried.
                                if self.cleanup_link(k.borrow(), false, barrier) {
                                    key = k;
                                    value = v;
                                    continue;
                                }
                                return Ok(InsertResult::Retry(k, v));
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
                let insert_result = unbounded.insert(key, value, async_wait, barrier)?;
                match insert_result {
                    InsertResult::Success
                    | InsertResult::Duplicate(..)
                    | InsertResult::Frozen(..) => return Ok(insert_result),
                    InsertResult::Full(k, v) => {
                        let split_result = self.split_node(
                            k,
                            v,
                            None,
                            unbounded_ptr,
                            &self.unbounded_child,
                            false,
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
                    InsertResult::Retired(k, v) => {
                        debug_assert!(unbounded.retired(Relaxed));
                        if self.coalesce(barrier) == RemoveResult::Retired {
                            debug_assert!(self.retired(Relaxed));
                            return Ok(InsertResult::Retired(k, v));
                        }
                        return Err((k, v));
                    }
                    InsertResult::Retry(k, v) => {
                        if self.cleanup_link(k.borrow(), false, barrier) {
                            key = k;
                            value = v;
                            continue;
                        }
                        return Ok(InsertResult::Retry(k, v));
                    }
                };
            }
            debug_assert!(unbounded_ptr.tag() == RETIRED);
            return Ok(InsertResult::Retired(key, value));
        }
    }

    /// Removes an entry associated with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is required with a boolean flag indicating that an entry has been removed.
    #[inline]
    pub(super) fn remove_if<Q, F: FnMut(&V) -> bool, D>(
        &self,
        key: &Q,
        condition: &mut F,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<RemoveResult, bool>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
        D: DeriveAsyncWait,
    {
        loop {
            let (child, metadata) = self.children.min_greater_equal(key);
            if let Some((_, child)) = child {
                let child_ptr = child.load(Acquire, barrier);
                if let Some(child) = child_ptr.as_ref() {
                    if self.children.validate(metadata) {
                        // Data race resolution - see `LeafNode::search`.
                        let result =
                            child.remove_if::<_, _, _>(key, condition, async_wait, barrier)?;
                        if result == RemoveResult::Cleanup {
                            if self.cleanup_link(key, false, barrier) {
                                return Ok(RemoveResult::Success);
                            }
                            return Ok(RemoveResult::Cleanup);
                        }
                        if result == RemoveResult::Retired {
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
                let result = unbounded.remove_if::<_, _, _>(key, condition, async_wait, barrier)?;
                if result == RemoveResult::Cleanup {
                    if self.cleanup_link(key, false, barrier) {
                        return Ok(RemoveResult::Success);
                    }
                    return Ok(RemoveResult::Cleanup);
                }
                if result == RemoveResult::Retired {
                    return Ok(self.coalesce(barrier));
                }
                return Ok(result);
            }
            return Ok(RemoveResult::Fail);
        }
    }

    /// Splits a full node.
    ///
    /// # Errors
    ///
    /// Returns an error if retry is required.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn split_node<D: DeriveAsyncWait>(
        &self,
        key: K,
        value: V,
        full_node_key: Option<&K>,
        full_node_ptr: Ptr<Node<K, V>>,
        full_node: &AtomicArc<Node<K, V>>,
        root_split: bool,
        async_wait: &mut D,
        barrier: &Barrier,
    ) -> Result<InsertResult<K, V>, (K, V)> {
        let target = full_node_ptr.as_ref().unwrap();
        if self.latch.compare_exchange(0, 1, Acquire, Relaxed).is_err() {
            target.rollback(barrier);
            self.wait(async_wait);
            return Err((key, value));
        }
        debug_assert!(!self.retired(Relaxed));

        if full_node_ptr != full_node.load(Relaxed, barrier) {
            self.latch.store(0, Release);
            self.wait_queue.signal();
            target.rollback(barrier);
            return Err((key, value));
        }

        self.split_op
            .origin_node
            .swap((full_node.get_arc(Relaxed, barrier), Tag::None), Relaxed);
        if let Some(full_node_key) = full_node_key {
            self.split_op
                .origin_node_key
                .store(full_node_key as *const K as *mut K, Relaxed);
        }

        match target.node() {
            Type::Internal(full_internal_node) => {
                // Copies nodes except for the known full node to the newly allocated internal node entries.
                let internal_nodes = (
                    Arc::new(Node::new_internal_node()),
                    Arc::new(Node::new_internal_node()),
                );
                let Type::Internal(low_key_nodes) = &internal_nodes.0.node() else {
                    unreachable!()
                };
                let Type::Internal(high_key_nodes) = &internal_nodes.1.node() else {
                    unreachable!()
                };

                // Builds a list of valid nodes.
                #[allow(clippy::type_complexity)]
                let mut entry_array: [Option<(Option<&K>, AtomicArc<Node<K, V>>)>;
                    DIMENSION.num_entries + 2] = Default::default();
                let mut num_entries = 0;
                for entry in Scanner::new(&full_internal_node.children) {
                    if unsafe {
                        full_internal_node
                            .split_op
                            .origin_node_key
                            .load(Relaxed)
                            .as_ref()
                            .map_or_else(|| false, |key| entry.0.borrow() == key)
                    } {
                        let low_key_node_ptr = full_internal_node
                            .split_op
                            .low_key_node
                            .load(Relaxed, barrier);
                        if !low_key_node_ptr.is_null() {
                            entry_array[num_entries].replace((
                                Some(unsafe {
                                    full_internal_node
                                        .split_op
                                        .middle_key
                                        .load(Relaxed)
                                        .as_ref()
                                        .unwrap()
                                }),
                                full_internal_node
                                    .split_op
                                    .low_key_node
                                    .clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                        let high_key_node_ptr = full_internal_node
                            .split_op
                            .high_key_node
                            .load(Relaxed, barrier);
                        if !high_key_node_ptr.is_null() {
                            entry_array[num_entries].replace((
                                Some(entry.0),
                                full_internal_node
                                    .split_op
                                    .high_key_node
                                    .clone(Relaxed, barrier),
                            ));
                            num_entries += 1;
                        }
                    } else {
                        entry_array[num_entries]
                            .replace((Some(entry.0), entry.1.clone(Relaxed, barrier)));
                        num_entries += 1;
                    }
                }
                if full_internal_node
                    .split_op
                    .origin_node_key
                    .load(Relaxed)
                    .is_null()
                {
                    // If the origin is an unbounded node, assign the high key node to the high key
                    // node's unbounded.
                    let low_key_node_ptr = full_internal_node
                        .split_op
                        .low_key_node
                        .load(Relaxed, barrier);
                    if !low_key_node_ptr.is_null() {
                        entry_array[num_entries].replace((
                            Some(unsafe {
                                full_internal_node
                                    .split_op
                                    .middle_key
                                    .load(Relaxed)
                                    .as_ref()
                                    .unwrap()
                            }),
                            full_internal_node
                                .split_op
                                .low_key_node
                                .clone(Relaxed, barrier),
                        ));
                        num_entries += 1;
                    }
                    let high_key_node_ptr = full_internal_node
                        .split_op
                        .high_key_node
                        .load(Relaxed, barrier);
                    if !high_key_node_ptr.is_null() {
                        entry_array[num_entries].replace((
                            None,
                            full_internal_node
                                .split_op
                                .high_key_node
                                .clone(Relaxed, barrier),
                        ));
                        num_entries += 1;
                    }
                } else {
                    // If the origin is a bounded node, assign the unbounded node to the high key
                    // node's unbounded.
                    entry_array[num_entries].replace((
                        None,
                        full_internal_node.unbounded_child.clone(Relaxed, barrier),
                    ));
                    num_entries += 1;
                }
                debug_assert!(num_entries >= 2);

                let low_key_node_array_size = num_entries / 2;
                for (i, entry) in entry_array.iter().enumerate() {
                    if let Some((k, v)) = entry {
                        match (i + 1).cmp(&low_key_node_array_size) {
                            Less => {
                                low_key_nodes.children.insert_unchecked(
                                    k.unwrap().clone(),
                                    v.clone(Relaxed, barrier),
                                    i,
                                );
                            }
                            Equal => {
                                if let Some(&k) = k.as_ref() {
                                    self.split_op
                                        .middle_key
                                        .store(k as *const K as *mut K, Relaxed);
                                }
                                low_key_nodes
                                    .unbounded_child
                                    .swap((v.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                            }
                            Greater => {
                                if let Some(k) = k.cloned() {
                                    high_key_nodes.children.insert_unchecked(
                                        k,
                                        v.clone(Relaxed, barrier),
                                        i - low_key_node_array_size,
                                    );
                                } else {
                                    high_key_nodes
                                        .unbounded_child
                                        .swap((v.get_arc(Relaxed, barrier), Tag::None), Relaxed);
                                }
                            }
                        };
                    } else {
                        break;
                    }
                }

                // Turns the new nodes into internal nodes.
                self.split_op
                    .low_key_node
                    .swap((Some(internal_nodes.0), Tag::None), Relaxed);
                self.split_op
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

                self.split_op.middle_key.store(
                    full_leaf_node.split_leaf_node(
                        low_key_leaf_node.unwrap(),
                        high_key_leaf_node.unwrap(),
                        barrier,
                    ) as *const K as *mut K,
                    Relaxed,
                );

                // Turns the new leaves into leaf nodes.
                self.split_op
                    .low_key_node
                    .swap((Some(leaf_nodes.0), Tag::None), Relaxed);
                self.split_op
                    .high_key_node
                    .swap((Some(leaf_nodes.1), Tag::None), Relaxed);
            }
        };

        // Inserts the newly allocated internal nodes into the main array.
        match self.children.insert(
            unsafe {
                self.split_op
                    .middle_key
                    .load(Relaxed)
                    .as_ref()
                    .unwrap()
                    .clone()
            },
            self.split_op.low_key_node.clone(Relaxed, barrier),
        ) {
            InsertResult::Success => (),
            InsertResult::Duplicate(..) | InsertResult::Frozen(..) | InsertResult::Retry(..) => {
                unreachable!()
            }
            InsertResult::Full(..) | InsertResult::Retired(..) => {
                // Insertion failed: expects that the parent splits this node.
                return Ok(InsertResult::Full(key, value));
            }
        };

        // Replace the full node with the high-key node.
        let unused_node = full_node
            .swap(
                (
                    self.split_op.high_key_node.get_arc(Relaxed, barrier),
                    Tag::None,
                ),
                Release,
            )
            .0;

        if root_split {
            // Return without unlocking it.
            return Ok(InsertResult::Retry(key, value));
        }

        // Unlock the node.
        self.finish_split();

        // Drop the deprecated nodes.
        if let Some(unused_node) = unused_node {
            // Clean up the split operation by committing it.
            unused_node.commit(barrier);
            let _ = unused_node.release(barrier);
        }

        // Since a new node has been inserted, the caller can retry.
        Ok(InsertResult::Retry(key, value))
    }

    /// Finishes splitting the [`InternalNode`].
    #[inline]
    pub(super) fn finish_split(&self) {
        self.split_op.reset();
        self.latch.store(0, Release);
        self.wait_queue.signal();
    }

    /// Commits an on-going structural change recursively.
    #[inline]
    pub(super) fn commit(&self, barrier: &Barrier) {
        let origin = self.split_op.reset();

        // Mark the internal node retired to prevent further locking attempts.
        self.latch.store(u8::MAX, Release);
        self.wait_queue.signal();
        if let Some(origin) = origin {
            origin.commit(barrier);
        }
    }

    /// Rolls back the ongoing split operation recursively.
    #[inline]
    pub(super) fn rollback(&self, barrier: &Barrier) {
        let origin = self.split_op.reset();
        self.latch.store(0, Release);
        self.wait_queue.signal();
        if let Some(origin) = origin {
            origin.rollback(barrier);
        }
    }

    /// Cleans up logically deleted [`LeafNode`] instances in the linked list.
    ///
    /// If the target leaf node does not exist in the sub-tree, returns `false`.
    #[inline]
    pub(super) fn cleanup_link<'b, Q>(
        &self,
        key: &Q,
        traverse_max: bool,
        barrier: &'b Barrier,
    ) -> bool
    where
        K: 'b + Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if traverse_max {
            // It just has to search for the maximum leaf node in the tree.
            if let Some(unbounded) = self.unbounded_child.load(Acquire, barrier).as_ref() {
                return unbounded.cleanup_link(key, true, barrier);
            }
        } else if let Some(child_scanner) = Scanner::max_less(&self.children, key) {
            if let Some((_, child)) = child_scanner.get() {
                if let Some(child) = child.load(Acquire, barrier).as_ref() {
                    return child.cleanup_link(key, true, barrier);
                }
            }
        }
        false
    }

    /// Waits for the lock on the [`LeafNode`] to be released.
    #[inline]
    pub(super) fn wait<D: DeriveAsyncWait>(&self, async_wait: &mut D) {
        let waiter = || {
            if self.latch.load(Relaxed) == 1 {
                // The `InternalNode` is being split or locked.
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

    /// Tries to coalesce nodes.
    fn coalesce<Q>(&self, barrier: &Barrier) -> RemoveResult
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node_deleted = false;
        while let Some(lock) = Locker::try_lock(self) {
            let mut max_key_entry = None;
            for (key, node) in Scanner::new(&self.children) {
                let node_ptr = node.load(Relaxed, barrier);
                let node_ref = node_ptr.as_ref().unwrap();
                if node_ref.retired(Relaxed) {
                    let result = self.children.remove_if(key.borrow(), &mut |_| true);
                    debug_assert_ne!(result, RemoveResult::Fail);

                    // Once the key is removed, it is safe to deallocate the node as the validation
                    // loop ensures the absence of readers.
                    if let Some(node) = node.swap((None, Tag::None), Release).0 {
                        let _ = node.release(barrier);
                        node_deleted = true;
                    }
                } else {
                    max_key_entry.replace((key, node));
                }
            }

            // The unbounded node is replaced with the maximum key node if retired.
            let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
            let fully_empty = if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.retired(Relaxed) {
                    if let Some((key, max_key_child)) = max_key_entry {
                        if let Some(obsolete_node) = self
                            .unbounded_child
                            .swap(
                                (max_key_child.get_arc(Relaxed, barrier), Tag::None),
                                Release,
                            )
                            .0
                        {
                            debug_assert!(obsolete_node.retired(Relaxed));
                            let _ = obsolete_node.release(barrier);
                            node_deleted = true;
                        }
                        let result = self.children.remove_if(key.borrow(), &mut |_| true);
                        debug_assert_ne!(result, RemoveResult::Fail);
                        if let Some(node) = max_key_child.swap((None, Tag::None), Release).0 {
                            let _ = node.release(barrier);
                            node_deleted = true;
                        }
                        false
                    } else {
                        if let Some(obsolete_node) =
                            self.unbounded_child.swap((None, RETIRED), Release).0
                        {
                            debug_assert!(obsolete_node.retired(Relaxed));
                            let _ = obsolete_node.release(barrier);
                            node_deleted = true;
                        }
                        true
                    }
                } else {
                    false
                }
            } else {
                debug_assert!(unbounded_ptr.tag() == RETIRED);
                true
            };

            if fully_empty {
                return RemoveResult::Retired;
            }

            drop(lock);
            if !self.has_retired_node(barrier) {
                break;
            }
        }

        if node_deleted {
            RemoveResult::Cleanup
        } else {
            RemoveResult::Success
        }
    }

    /// Checks if the [`InternalNode`] has a retired [`Node`].
    fn has_retired_node(&self, barrier: &Barrier) -> bool {
        let mut has_valid_node = false;
        for entry in Scanner::new(&self.children) {
            let leaf_ptr = entry.1.load(Relaxed, barrier);
            if let Some(leaf) = leaf_ptr.as_ref() {
                if leaf.retired(Relaxed) {
                    return true;
                }
                has_valid_node = true;
            }
        }
        if !has_valid_node {
            let unbounded_ptr = self.unbounded_child.load(Relaxed, barrier);
            if let Some(unbounded) = unbounded_ptr.as_ref() {
                if unbounded.retired(Relaxed) {
                    return true;
                }
            }
        }
        false
    }
}

/// [`Locker`] holds exclusive access to a [`InternalNode`].
pub struct Locker<'n, K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    internal_node: &'n InternalNode<K, V>,
}

impl<'n, K, V> Locker<'n, K, V>
where
    K: Clone + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Acquires exclusive lock on the [`InternalNode`].
    #[inline]
    pub(super) fn try_lock(internal_node: &'n InternalNode<K, V>) -> Option<Locker<'n, K, V>> {
        if internal_node
            .latch
            .compare_exchange(0, 1, Acquire, Relaxed)
            .is_ok()
        {
            Some(Locker { internal_node })
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
        debug_assert_eq!(self.internal_node.latch.load(Relaxed), 1);
        self.internal_node.latch.store(0, Release);
        self.internal_node.wait_queue.signal();
    }
}

/// [`StructuralChange`] stores intermediate results during a split operation.
///
/// `AtomicPtr` members may point to values under the protection of the [`Barrier`] used for the
/// split operation.
struct StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    origin_node_key: AtomicPtr<K>,
    origin_node: AtomicArc<Node<K, V>>,
    low_key_node: AtomicArc<Node<K, V>>,
    middle_key: AtomicPtr<K>,
    high_key_node: AtomicArc<Node<K, V>>,
}

impl<K, V> StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    fn reset(&self) -> Option<Arc<Node<K, V>>> {
        self.origin_node_key.store(ptr::null_mut(), Relaxed);
        self.low_key_node.swap((None, Tag::None), Relaxed);
        self.middle_key.store(ptr::null_mut(), Relaxed);
        self.high_key_node.swap((None, Tag::None), Relaxed);
        self.origin_node.swap((None, Tag::None), Relaxed).0
    }
}

impl<K, V> Default for StructuralChange<K, V>
where
    K: 'static + Clone + Ord + Send + Sync,
    V: 'static + Clone + Send + Sync,
{
    #[inline]
    fn default() -> Self {
        Self {
            origin_node_key: AtomicPtr::default(),
            origin_node: AtomicArc::null(),
            low_key_node: AtomicArc::null(),
            middle_key: AtomicPtr::default(),
            high_key_node: AtomicArc::null(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;

    use tokio::sync;

    fn new_level_3_node() -> InternalNode<usize, usize> {
        InternalNode {
            children: Leaf::new(),
            unbounded_child: AtomicArc::new(Node {
                node: Type::Internal(InternalNode {
                    children: Leaf::new(),
                    unbounded_child: AtomicArc::new(Node::new_leaf_node()),
                    split_op: StructuralChange::default(),
                    latch: AtomicU8::new(0),
                    wait_queue: WaitQueue::default(),
                }),
            }),
            split_op: StructuralChange::default(),
            latch: AtomicU8::new(0),
            wait_queue: WaitQueue::default(),
        }
    }

    #[test]
    fn bulk() {
        let internal_node = new_level_3_node();
        let barrier = Barrier::new();
        assert_eq!(internal_node.depth(1, &barrier), 3);

        for k in 0..8192 {
            match internal_node.insert(k, k, &mut (), &barrier) {
                Ok(result) => match result {
                    InsertResult::Success => {
                        assert_eq!(internal_node.search(&k, &barrier), Some(&k));
                    }
                    InsertResult::Duplicate(..)
                    | InsertResult::Frozen(..)
                    | InsertResult::Retired(..) => unreachable!(),
                    InsertResult::Full(_, _) => {
                        internal_node.rollback(&barrier);
                        for j in 0..k {
                            assert_eq!(internal_node.search(&j, &barrier), Some(&j));
                            if j == k - 1 {
                                assert!(matches!(
                                    internal_node.remove_if::<_, _, _>(
                                        &j,
                                        &mut |_| true,
                                        &mut (),
                                        &barrier
                                    ),
                                    Ok(RemoveResult::Retired)
                                ));
                            } else {
                                assert!(internal_node
                                    .remove_if::<_, _, _>(&j, &mut |_| true, &mut (), &barrier)
                                    .is_ok(),);
                            }
                            assert_eq!(internal_node.search(&j, &barrier), None);
                        }
                        break;
                    }
                    InsertResult::Retry(k, v) => {
                        let result = internal_node.insert(k, v, &mut (), &barrier);
                        assert!(result.is_ok());
                        assert_eq!(internal_node.search(&k, &barrier), Some(&k));
                    }
                },
                Err((k, v)) => {
                    let result = internal_node.insert(k, v, &mut (), &barrier);
                    assert!(result.is_ok());
                    assert_eq!(internal_node.search(&k, &barrier), Some(&k));
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel() {
        let num_tasks = 8;
        let workload_size = 64;
        let barrier = Arc::new(sync::Barrier::new(num_tasks));
        for _ in 0..64 {
            let internal_node = Arc::new(new_level_3_node());
            assert!(internal_node
                .insert(usize::MAX, usize::MAX, &mut (), &Barrier::new())
                .is_ok());
            let mut task_handles = Vec::with_capacity(num_tasks);
            for task_id in 0..num_tasks {
                let barrier_cloned = barrier.clone();
                let internal_node_cloned = internal_node.clone();
                task_handles.push(tokio::task::spawn(async move {
                    barrier_cloned.wait().await;
                    let barrier = Barrier::new();
                    let mut max_key = None;
                    let range = (task_id * workload_size)..((task_id + 1) * workload_size);
                    for id in range.clone() {
                        loop {
                            if let Ok(r) = internal_node_cloned.insert(id, id, &mut (), &barrier) {
                                match r {
                                    InsertResult::Success => {
                                        match internal_node_cloned.insert(id, id, &mut (), &barrier)
                                        {
                                            Ok(InsertResult::Duplicate(..)) | Err(_) => (),
                                            _ => unreachable!(),
                                        }
                                        break;
                                    }
                                    InsertResult::Full(..) => {
                                        internal_node_cloned.rollback(&barrier);
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
                        assert_eq!(internal_node_cloned.search(&id, &barrier), Some(&id));
                    }
                    for id in range {
                        if max_key.map_or(false, |m| m == id) {
                            break;
                        }
                        let mut removed = false;
                        loop {
                            match internal_node_cloned.remove_if::<_, _, _>(
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
                        assert!(internal_node_cloned.search(&id, &barrier).is_none());
                        if let Ok(RemoveResult::Success) = internal_node_cloned
                            .remove_if::<_, _, _>(&id, &mut |_| true, &mut (), &barrier)
                        {
                            unreachable!()
                        }
                    }
                }));
            }

            for r in futures::future::join_all(task_handles).await {
                assert!(r.is_ok());
            }
            assert!(internal_node
                .remove_if::<_, _, _>(&usize::MAX, &mut |_| true, &mut (), &Barrier::new())
                .is_ok());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn durability() {
        let num_tasks = 16_usize;
        let num_iterations = 64;
        let workload_size = 64_usize;
        for k in 0..64 {
            let fixed_point = k * 16;
            for _ in 0..=num_iterations {
                let barrier = Arc::new(sync::Barrier::new(num_tasks));
                let internal_node = Arc::new(new_level_3_node());
                let inserted: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
                let mut task_handles = Vec::with_capacity(num_tasks);
                for _ in 0..num_tasks {
                    let barrier_clone = barrier.clone();
                    let internal_node_clone = internal_node.clone();
                    let inserted_clone = inserted.clone();
                    task_handles.push(tokio::spawn(async move {
                        {
                            barrier_clone.wait().await;
                            let barrier = Barrier::new();
                            match internal_node_clone.insert(
                                fixed_point,
                                fixed_point,
                                &mut (),
                                &barrier,
                            ) {
                                Ok(InsertResult::Success) => {
                                    assert!(!inserted_clone.swap(true, Relaxed));
                                }
                                Ok(InsertResult::Full(_, _) | InsertResult::Retired(_, _)) => {
                                    internal_node_clone.rollback(&barrier);
                                }
                                _ => (),
                            };
                            assert_eq!(
                                internal_node_clone.search(&fixed_point, &barrier).unwrap(),
                                &fixed_point
                            );
                        }
                        {
                            barrier_clone.wait().await;
                            let barrier = Barrier::new();
                            for i in 0..workload_size {
                                if i != fixed_point {
                                    if let Ok(
                                        InsertResult::Full(_, _) | InsertResult::Retired(_, _),
                                    ) = internal_node_clone.insert(i, i, &mut (), &barrier)
                                    {
                                        internal_node_clone.rollback(&barrier);
                                    }
                                }
                                assert_eq!(
                                    internal_node_clone.search(&fixed_point, &barrier).unwrap(),
                                    &fixed_point
                                );
                            }
                            for i in 0..workload_size {
                                let max_scanner = internal_node_clone
                                    .max_le_appr(&fixed_point, &barrier)
                                    .unwrap();
                                assert!(*max_scanner.get().unwrap().0 <= fixed_point);
                                let mut min_scanner = internal_node_clone.min(&barrier).unwrap();
                                if let Some((f, v)) = min_scanner.next() {
                                    assert_eq!(*f, *v);
                                    assert!(*f <= fixed_point);
                                } else {
                                    let (f, v) =
                                        min_scanner.jump(None, &barrier).unwrap().get().unwrap();
                                    assert_eq!(*f, *v);
                                    assert!(*f <= fixed_point);
                                }
                                let _result = internal_node_clone.remove_if::<_, _, _>(
                                    &i,
                                    &mut |v| *v != fixed_point,
                                    &mut (),
                                    &barrier,
                                );
                                assert_eq!(
                                    internal_node_clone.search(&fixed_point, &barrier).unwrap(),
                                    &fixed_point
                                );
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
