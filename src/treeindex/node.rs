use super::leaf::{LeafScanner, ARRAY_SIZE};
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

enum Error<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: returns a newly allocated node for the parent to consume
    Full((K, V)),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// InternalNode: |ptr(children)/max(child keys)|...|ptr(children)|
    InternalNode {
        bounded_children: Leaf<K, Atomic<Node<K, V>>>,
        unbounded_child: Atomic<Node<K, V>>,
        reserved_low_key: Atomic<Node<K, V>>,
        reserved_high_key: Atomic<Node<K, V>>,
    },
    /// LeafNode: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
    LeafNode {
        bounded_children: Leaf<K, Atomic<Leaf<K, V>>>,
        unbounded_child: Atomic<Leaf<K, V>>,
        reserved_low_key: Atomic<Leaf<K, V>>,
        reserved_high_key: Atomic<Leaf<K, V>>,
    },
}

pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
    side_link: Atomic<Node<K, V>>,
    floor: usize,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor > 0 {
                NodeType::InternalNode {
                    bounded_children: Leaf::new(),
                    unbounded_child: Atomic::null(),
                    reserved_low_key: Atomic::null(),
                    reserved_high_key: Atomic::null(),
                }
            } else {
                NodeType::LeafNode {
                    bounded_children: Leaf::new(),
                    unbounded_child: Atomic::null(),
                    reserved_low_key: Atomic::null(),
                    reserved_high_key: Atomic::null(),
                }
            },
            side_link: Atomic::null(),
            floor,
        }
    }

    pub fn search<'a>(&'a self, key: &K, guard: &'a Guard) -> Option<LeafNodeScanner<'a, K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                if let Some((_, child)) = bounded_children.min_ge(&key) {
                    unsafe { child.load(Acquire, guard).deref().search(key, guard) }
                } else {
                    let current_tail_node = unbounded_child.load(Relaxed, guard);
                    if current_tail_node.is_null() {
                        return None;
                    }
                    unsafe { current_tail_node.deref().search(key, guard) }
                }
            }
            NodeType::LeafNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                if let Some((_, child)) = bounded_children.min_ge(&key) {
                    None //unsafe { child.load(Acquire, guard).deref().search(key) }
                } else {
                    let current_tail_node = unbounded_child.load(Relaxed, guard);
                    if current_tail_node.is_null() {
                        return None;
                    }
                    None //unsafe { current_tail_node.deref().search(key) }
                }
            }
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    /// B+ tree assures that the tree is filled up from the very bottom nodes.
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), Error<K, V>> {
        match &self.entry {
            NodeType::InternalNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                loop {
                    if let Some((_, child)) = bounded_children.min_ge(&key) {
                        let child_node = child.load(Acquire, guard);
                        let result = unsafe { child_node.deref().insert(key, value, guard) };
                        return self.handle_result(result, child_node, guard);
                    } else if !bounded_children.full() {
                        if let Some(result) = bounded_children.insert(
                            key.clone(),
                            Atomic::new(Node::new(self.floor - 1)),
                            false,
                        ) {
                            drop(unsafe { (result.0).1.into_owned() });
                        }
                    } else {
                        break;
                    }
                }
                let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                if current_tail_node.is_null() {
                    match unbounded_child.compare_and_set(
                        current_tail_node,
                        Owned::new(Node::new(self.floor - 1)),
                        Relaxed,
                        guard,
                    ) {
                        Ok(result) => current_tail_node = result,
                        Err(result) => current_tail_node = result.current,
                    }
                }
                let result = unsafe { current_tail_node.deref().insert(key, value, guard) };
                self.handle_result(result, current_tail_node, guard)
            }
            NodeType::LeafNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                loop {
                    if let Some((_, child)) = bounded_children.min_ge(&key) {
                        let child_node = child.load(Acquire, guard);
                        return unsafe { child_node.deref().insert(key, value, false) }
                            .map_or_else(
                                || Ok(()),
                                |result| {
                                    if result.1 {
                                        Err(Error::Duplicated(result.0))
                                    } else {
                                        self.split_leaf(
                                            result.0,
                                            false,
                                            &bounded_children,
                                            &child,
                                            &reserved_low_key,
                                            &reserved_high_key,
                                            guard,
                                        )
                                    }
                                },
                            );
                    } else if !bounded_children.full() {
                        if let Some(result) =
                            bounded_children.insert(key.clone(), Atomic::new(Leaf::new()), false)
                        {
                            drop(unsafe { (result.0).1.into_owned() });
                        }
                    } else {
                        break;
                    }
                }
                let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                if current_tail_node.is_null() {
                    match unbounded_child.compare_and_set(
                        current_tail_node,
                        Owned::new(Leaf::new()),
                        Relaxed,
                        guard,
                    ) {
                        Ok(result) => current_tail_node = result,
                        Err(result) => current_tail_node = result.current,
                    }
                }
                return unsafe { current_tail_node.deref().insert(key, value, false) }.map_or_else(
                    || Ok(()),
                    |result| {
                        if result.1 {
                            Err(Error::Duplicated(result.0))
                        } else {
                            self.split_leaf(
                                result.0,
                                true,
                                &bounded_children,
                                &unbounded_child,
                                &reserved_low_key,
                                &reserved_high_key,
                                guard,
                            )
                        }
                    },
                );
            }
        }
    }

    fn split_leaf(
        &self,
        entry: (K, V),
        is_undounded: bool,
        leaf_array: &Leaf<K, Atomic<Leaf<K, V>>>,
        full_leaf: &Atomic<Leaf<K, V>>,
        low_key: &Atomic<Leaf<K, V>>,
        high_key: &Atomic<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        debug_assert!(unsafe { full_leaf.load(Acquire, &guard).deref().full() });
        let new_leaf_low_key = Owned::new(Leaf::new());
        let new_leaf_high_key = Owned::new(Leaf::new());
        let mut low_key_leaf = Shared::null();
        let mut high_key_leaf = Shared::null();
        match low_key.compare_and_set(Shared::null(), new_leaf_low_key, Relaxed, guard) {
            Ok(result) => low_key_leaf = result,
            Err(_) => return Err(Error::Retry(entry)),
        }
        match high_key.compare_and_set(Shared::null(), new_leaf_high_key, Relaxed, guard) {
            Ok(result) => high_key_leaf = result,
            Err(_) => {
                drop(unsafe { low_key.swap(Shared::null(), Relaxed, guard).into_owned() });
                return Err(Error::Retry(entry));
            }
        }

        // copy entries to the newly allocated leaves
        let mut iterated = 0;
        let mut scanner = LeafScanner::new(unsafe { full_leaf.load(Acquire, &guard).deref() });
        while let Some(entry) = scanner.next() {
            if iterated < ARRAY_SIZE / 2 {
                unsafe {
                    low_key_leaf
                        .deref()
                        .insert(entry.0.clone(), entry.1.clone(), false);
                }
            } else {
                unsafe {
                    high_key_leaf
                        .deref()
                        .insert(entry.0.clone(), entry.1.clone(), false);
                }
            }
            iterated += 1;
        }

        // insert the given entry
        let high_key_leaf_empty = iterated <= ARRAY_SIZE / 2;
        if high_key_leaf_empty || unsafe { low_key_leaf.deref().min_ge(&entry.0).is_some() } {
            // insert the entry into the low-key leaf if the high-key leaf is empty, or the key fits the low-key leaf
            unsafe {
                low_key_leaf
                    .deref()
                    .insert(entry.0.clone(), entry.1.clone(), false)
            };
        } else {
            // insert the entry into the high-key leaf
            unsafe {
                high_key_leaf
                    .deref()
                    .insert(entry.0.clone(), entry.1.clone(), false)
            };
        }

        // if the key is for the unbounded child leaf, return
        if is_undounded {
            return Err(Error::Full(entry));
        }

        // insert the newly added leaf into the main array
        if high_key_leaf_empty {
            // replace the full leaf with the low-key leaf
            let old_full_leaf = full_leaf.swap(low_key_leaf, Release, &guard);
            // deallocate the deprecated leaf
            unsafe {
                guard.defer_destroy(old_full_leaf);
            };
            // everything's done
            let unused_high_key_leaf = high_key.swap(Shared::null(), Release, guard);
            drop(unsafe { unused_high_key_leaf.into_owned() });

            // it is practically un-locking the leaf node
            low_key.swap(Shared::null(), Release, guard);

            // OK
            return Ok(());
        } else {
            let max_key = unsafe { low_key_leaf.deref().max_key() }.unwrap();
            if leaf_array
                .insert(max_key.clone(), Atomic::from(low_key_leaf), false)
                .is_some()
            {
                // insertion failed: expect that the caller handles the situation
                return Err(Error::Full(entry));
            }

            // replace the full leaf with the high-key leaf
            let old_full_leaf = full_leaf.swap(high_key_leaf, Release, &guard);
            // deallocate the deprecated leaf
            unsafe {
                guard.defer_destroy(old_full_leaf);
            };

            // it is practically un-locking the leaf node
            low_key.swap(Shared::null(), Release, guard);

            // OK
            return Ok(());
        }
    }

    fn handle_result(
        &self,
        result: Result<(), Error<K, V>>,
        child_node: Shared<Node<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
        match result {
            Ok(_) => return Ok(()),
            Err(err) => match err {
                Error::Duplicated(_) => return Err(err),
                Error::Full(_) => {
                    // try to split
                    // split the entry into two new entries => insert the new one => replace the old one with the new one
                    // return self.split_and_insert_locked(entry, child);
                    return Ok(());
                }
                Error::Retry(_) => return Err(err),
            },
        }
    }
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        match &self.entry {
            NodeType::InternalNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                let mut scanner = LeafScanner::new(&bounded_children);
                while let Some(entry) = scanner.next() {
                    let child = entry.1.swap(Shared::null(), Acquire, &guard);
                    unsafe { guard.defer_destroy(child) };
                }
                let tail = unbounded_child.load(Acquire, &guard);
                unsafe { guard.defer_destroy(tail) };
                let low_key_child = reserved_low_key.swap(Shared::null(), Relaxed, &guard);
                if !low_key_child.is_null() {
                    drop(unsafe { low_key_child.into_owned() });
                }
                let high_key_child = reserved_high_key.swap(Shared::null(), Relaxed, &guard);
                if !high_key_child.is_null() {
                    drop(unsafe { high_key_child.into_owned() });
                }
            }
            NodeType::LeafNode {
                bounded_children,
                unbounded_child,
                reserved_low_key,
                reserved_high_key,
            } => {
                let mut scanner = LeafScanner::new(&bounded_children);
                while let Some(entry) = scanner.next() {
                    let child = entry.1.swap(Shared::null(), Acquire, &guard);
                    unsafe { guard.defer_destroy(child) };
                }
                let tail = unbounded_child.load(Acquire, &guard);
                unsafe { guard.defer_destroy(tail) };
                let low_key_child = reserved_low_key.swap(Shared::null(), Relaxed, &guard);
                if !low_key_child.is_null() {
                    drop(unsafe { low_key_child.into_owned() });
                }
                let high_key_child = reserved_high_key.swap(Shared::null(), Relaxed, &guard);
                if !high_key_child.is_null() {
                    drop(unsafe { high_key_child.into_owned() });
                }
            }
        }
    }
}

pub struct LeafNodeScanner<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    leaf_node: &'a Node<K, V>,
    node_scanner: Option<LeafScanner<'a, K, Atomic<Leaf<K, V>>>>,
    leaf_scanner: Option<LeafScanner<'a, K, V>>,
    guard: &'a Guard,
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> LeafNodeScanner<'a, K, V> {
    fn new(leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: None,
            guard,
        }
    }
    fn from(key: &K, leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        // TODO
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: None,
            guard,
        }
    }
    fn from_ge(key: &K, leaf_node: &'a Node<K, V>, guard: &'a Guard) -> LeafNodeScanner<'a, K, V> {
        // TODO
        LeafNodeScanner::<'a, K, V> {
            leaf_node,
            node_scanner: None,
            leaf_scanner: None,
            guard,
        }
    }
}

impl<'a, K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Iterator
    for LeafNodeScanner<'a, K, V>
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
            // leaf iteration
            if let Some(entry) = leaf_scanner.next() {
                return Some(entry);
            }
            // end of iteration
            if self.node_scanner.is_none() {
                return None;
            }
        }

        if self.node_scanner.is_none() && self.leaf_scanner.is_none() {
            // start scanning
            match &self.leaf_node.entry {
                NodeType::InternalNode {
                    bounded_children: _,
                    unbounded_child: _,
                    reserved_low_key: _,
                    reserved_high_key: _,
                } => return None,
                NodeType::LeafNode {
                    bounded_children,
                    unbounded_child: _,
                    reserved_low_key: _,
                    reserved_high_key: _,
                } => {
                    self.node_scanner
                        .replace(LeafScanner::new(bounded_children));
                }
            }
        }

        if let Some(node_scanner) = self.node_scanner.as_mut() {
            // proceed to the next leaf
            while let Some(leaf) = node_scanner.next() {
                self.leaf_scanner.replace(LeafScanner::new(unsafe {
                    leaf.1.load(Acquire, self.guard).deref()
                }));
                if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                    // leaf iteration
                    if let Some(entry) = leaf_scanner.next() {
                        return Some(entry);
                    }
                }
                self.leaf_scanner.take();
            }
        }
        self.node_scanner.take();

        let unbounded_child = match &self.leaf_node.entry {
            NodeType::InternalNode {
                bounded_children: _,
                unbounded_child: _,
                reserved_low_key: _,
                reserved_high_key: _,
            } => Shared::null(),
            NodeType::LeafNode {
                bounded_children: _,
                unbounded_child,
                reserved_low_key: _,
                reserved_high_key: _,
            } => unbounded_child.load(Acquire, self.guard),
        };
        if !unbounded_child.is_null() {
            self.leaf_scanner
                .replace(LeafScanner::new(unsafe { unbounded_child.deref() }));
            if let Some(leaf_scanner) = self.leaf_scanner.as_mut() {
                // leaf iteration
                if let Some(entry) = leaf_scanner.next() {
                    return Some(entry);
                }
            }
        }

        // end of iteration
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn leaf_node() {
        let guard = crossbeam_epoch::pin();
        // sequential
        let node = Node::new(0);
        for i in 0..ARRAY_SIZE {
            for j in 0..(ARRAY_SIZE + 1) {
                assert!(node
                    .insert((j + 1) * (ARRAY_SIZE + 1) - i, 10, &guard)
                    .is_ok());
                match node.insert((j + 1) * (ARRAY_SIZE + 1) - i, 11, &guard) {
                    Ok(_) => assert!(false),
                    Err(result) => match result {
                        Error::Duplicated(entry) => {
                            assert_eq!(entry, ((j + 1) * (ARRAY_SIZE + 1) - i, 11))
                        }
                        Error::Full(_) => assert!(false),
                        Error::Retry(_) => assert!(false),
                    },
                }
            }
        }
        match node.insert(0, 11, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(entry) => assert_eq!(entry, (0, 11)),
                Error::Retry(_) => assert!(false),
            },
        }
        match node.insert(240, 11, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(_) => assert!(false),
                Error::Retry(entry) => assert_eq!(entry, (240, 11)),
            },
        }
        // induce split
        let node = Node::new(0);
        for i in 0..ARRAY_SIZE {
            for j in 0..ARRAY_SIZE {
                if j == ARRAY_SIZE / 2 {
                    continue;
                }
                assert!(node
                    .insert((j + 1) * (ARRAY_SIZE + 1) - i, 10, &guard)
                    .is_ok());
                match node.insert((j + 1) * (ARRAY_SIZE + 1) - i, 11, &guard) {
                    Ok(_) => assert!(false),
                    Err(result) => match result {
                        Error::Duplicated(entry) => {
                            assert_eq!(entry, ((j + 1) * (ARRAY_SIZE + 1) - i, 11))
                        }
                        Error::Full(_) => assert!(false),
                        Error::Retry(_) => assert!(false),
                    },
                }
            }
        }
        for i in 0..(ARRAY_SIZE / 2) {
            assert!(node
                .insert((ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i, 10, &guard)
                .is_ok());
            match node.insert((ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i, 11, &guard) {
                Ok(_) => assert!(false),
                Err(result) => match result {
                    Error::Duplicated(entry) => {
                        assert_eq!(entry, ((ARRAY_SIZE / 2 + 1) * (ARRAY_SIZE + 1) - i, 11))
                    }
                    Error::Full(_) => assert!(false),
                    Error::Retry(_) => assert!(false),
                },
            }
        }
        for i in 0..ARRAY_SIZE {
            assert!(node
                .insert((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 10, &guard)
                .is_ok());
            match node.insert((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 11, &guard) {
                Ok(_) => assert!(false),
                Err(result) => match result {
                    Error::Duplicated(entry) => {
                        assert_eq!(entry, ((ARRAY_SIZE + 2) * (ARRAY_SIZE + 1) - i, 11))
                    }
                    Error::Full(_) => assert!(false),
                    Error::Retry(_) => assert!(false),
                },
            }
        }
        match node.insert(240, 11, &guard) {
            Ok(_) => assert!(false),
            Err(result) => match result {
                Error::Duplicated(_) => assert!(false),
                Error::Full(_) => assert!(false),
                Error::Retry(entry) => assert_eq!(entry, (240, 11)),
            },
        }

        let mut scanner = LeafNodeScanner::new(&node, &guard);
        let mut iterated = 0;
        let mut prev = 0;
        while let Some(entry) = scanner.next() {
            assert!(prev < *entry.0);
            assert_eq!(*entry.1, 10);
            prev = *entry.0;
            iterated += 1;
        }
        assert_eq!(iterated, ARRAY_SIZE * (ARRAY_SIZE + 1) - ARRAY_SIZE / 2);
    }
}
