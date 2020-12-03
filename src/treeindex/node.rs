use super::leaf::Scanner;
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned};
use std::cmp::Ordering;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    InternalNode((Leaf<K, Atomic<Node<K, V>>>, Atomic<Node<K, V>>)),
    LeafNode(Leaf<K, V>),
}

pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
    floor: usize,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor == 0 {
                NodeType::LeafNode(Leaf::new(None, Atomic::null()))
            } else {
                NodeType::InternalNode((Leaf::new(None, Atomic::null()), Atomic::null()))
            },
            floor,
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    /// The layout of a node: |child/max_key|child/max_key|...child/max_key|child
    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Option<(K, V)> {
        match &self.entry {
            NodeType::InternalNode(node) => {
                // firstly, try to insert into an existing child node
                let mut scanner = Scanner::new(&node.0, false, guard);
                while let Some(entry) = scanner.next() {
                    if (*entry.0).cmp(&key) != Ordering::Less {
                        let child_node = (*entry.1).load(Acquire, guard);
                        return unsafe { child_node.deref().insert(key, value, guard) };
                    }
                } // TODO: leaf.ceil(key)
                drop(scanner);

                // secondly, try to create a new child node
                let mut new_child_node = Atomic::null();
                let mut key_cloned = key.clone();
                if !node.0.full() {
                    new_child_node = Atomic::new(Node::new(self.floor - 1));
                    let inserted = node.0.insert(
                        key_cloned,
                        Atomic::from(new_child_node.load(Relaxed, guard)),
                        false,
                    );
                    if inserted.is_none() {
                        return unsafe {
                            new_child_node
                                .load(Relaxed, guard)
                                .deref()
                                .insert(key, value, guard)
                        };
                    } else {
                        // TODO: leaf.ceil(key) again;
                        // try to reuse the newly allocated child node as the tail entry
                        let (key, new_child_node) = inserted.unwrap();
                        let mut current_tail_node = node.1.load(Relaxed, guard);
                        if current_tail_node.is_null() {
                            if let Err(result) = node.1.compare_and_set(
                                current_tail_node,
                                new_child_node.load(Relaxed, guard),
                                Relaxed,
                                &guard,
                            ) {
                                unsafe {
                                    guard.defer_destroy(
                                        new_child_node.into_owned().into_shared(&guard),
                                    )
                                };
                                current_tail_node = result.current;
                            }
                        } else {
                            unsafe {
                                guard.defer_destroy(new_child_node.into_owned().into_shared(&guard))
                            };
                        }
                    }
                }
                // lastly, try to insert into the tail entry
                None
            }
            NodeType::LeafNode(node) => node.insert(key, value, false),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
}
