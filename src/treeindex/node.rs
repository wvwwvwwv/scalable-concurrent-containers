use super::leaf::Scanner;
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

enum Error<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// Duplicated key found: returns the given key-value pair.
    Duplicated((K, V)),
    /// Full: returns a newly allocated node for the parent to consume
    Full(Atomic<Node<K, V>>),
    /// Retry: return the given key-value pair.
    Retry((K, V)),
}

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// InternalNode: |ptr(children)/max(child keys)|...|ptr(children)|
    InternalNode {
        bounded_children: Leaf<K, Atomic<Node<K, V>>>,
        unbounded_child: Atomic<Node<K, V>>,
    },
    /// LeafNode: |ptr(entry array)/max(child keys)|...|ptr(entry array)|
    LeafNode {
        bounded_children: Leaf<K, Atomic<Leaf<K, V>>>,
        unbounded_child: Atomic<Leaf<K, V>>,
    },
}

pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
    side_link: Atomic<Node<K, V>>,
    lock: AtomicBool,
    floor: usize,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor > 0 {
                NodeType::InternalNode {
                    bounded_children: Leaf::new(),
                    unbounded_child: Atomic::null(),
                }
            } else {
                NodeType::LeafNode {
                    bounded_children: Leaf::new(),
                    unbounded_child: Atomic::null(),
                }
            },
            side_link: Atomic::null(),
            lock: AtomicBool::new(false),
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
                        &guard,
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
                                            // self.split_child()
                                            Ok(())
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
                            &guard,
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
                                    //
                                    Ok(())
                                }
                            },
                        );
                }
            }
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
                Error::Full(entry) => {
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
            } => {
                let mut scanner = Scanner::new(&bounded_children);
                while let Some(entry) = scanner.next() {
                    let child = entry.1.swap(Shared::null(), Acquire, &guard);
                    unsafe { guard.defer_destroy(child) };
                }
                let tail = unbounded_child.load(Acquire, &guard);
                unsafe { guard.defer_destroy(tail) };
            }
            NodeType::LeafNode {
                bounded_children,
                unbounded_child,
            } => {
                let mut scanner = Scanner::new(&bounded_children);
                while let Some(entry) = scanner.next() {
                    let child = entry.1.swap(Shared::null(), Acquire, &guard);
                    unsafe { guard.defer_destroy(child) };
                }
                let tail = unbounded_child.load(Acquire, &guard);
                unsafe { guard.defer_destroy(tail) };
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
