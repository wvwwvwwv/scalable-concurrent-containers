use super::leaf::{Scanner, ARRAY_SIZE};
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
                // firstly, try to insert into an existing child node
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

                // secondly, try to insert into the tail
                let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                if current_tail_node.is_null() {
                    if let Err(result) = unbounded_child.compare_and_set(
                        current_tail_node,
                        Owned::new(Node::new(self.floor - 1)),
                        Relaxed,
                        guard,
                    ) {
                        current_tail_node = result.current;
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
                {
                    // firstly, try to insert into an existing child node
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
                            if let Some(result) = bounded_children.insert(
                                key.clone(),
                                Atomic::new(Leaf::new()),
                                false,
                            ) {
                                drop(unsafe { (result.0).1.into_owned() });
                            }
                        } else {
                            break;
                        }
                    }
                    // secondly, try to insert into the tail
                    let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                    if current_tail_node.is_null() {
                        if let Err(result) = unbounded_child.compare_and_set(
                            current_tail_node,
                            Owned::new(Leaf::new()),
                            Relaxed,
                            guard,
                        ) {
                            current_tail_node = result.current;
                        }
                    }
                    return unsafe { current_tail_node.deref().insert(key, value, false) }
                        .map_or_else(
                            || Ok(()),
                            |result| {
                                if result.1 {
                                    Err(Error::Duplicated(result.0))
                                } else {
                                    // [TODO]
                                    Ok(())
                                }
                            },
                        );
                }
            }
        }
    }

    fn split_leaf(
        &self,
        entry: (K, V),
        leaf_array: &Leaf<K, Atomic<Leaf<K, V>>>,
        full_leaf: &Atomic<Leaf<K, V>>,
        low_key: &Atomic<Leaf<K, V>>,
        high_key: &Atomic<Leaf<K, V>>,
        guard: &Guard,
    ) -> Result<(), Error<K, V>> {
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
        let mut scanner = Scanner::new(unsafe { full_leaf.load(Acquire, &guard).deref() });
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
                let mut scanner = Scanner::new(&bounded_children);
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
                let mut scanner = Scanner::new(&bounded_children);
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

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn insert() {
        let guard = crossbeam_epoch::pin();
        let node = Node::new(0);
        assert!(node.insert(10, 10, &guard).is_ok());
        assert!(node.insert(10, 11, &guard).is_err());
    }
}
