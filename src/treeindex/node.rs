use super::leaf::Scanner;
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Owned};
use std::cmp::Ordering;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    /// InternalNode: |ptr(children)/max(child keys)|...|ptr(children)|
    InternalNode {
        bounded_children: Leaf<K, Atomic<Node<K, V>>>,
        unbounded_child: Atomic<Node<K, V>>,
    },
    /// LeafNode: |ptr(key_value_array)|ptr(next_leaf_node)|
    LeafNode {
        entry_array: Atomic<Leaf<K, V>>,
        side_link: Atomic<Node<K, V>>,
    },
}

pub struct Node<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    entry: NodeType<K, V>,
    floor: usize,
}

impl<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> Node<K, V> {
    pub fn new(floor: usize) -> Node<K, V> {
        Node {
            entry: if floor == 0 {
                NodeType::LeafNode {
                    entry_array: Atomic::new(Leaf::new(None)),
                    side_link: Atomic::null(),
                }
            } else {
                NodeType::InternalNode {
                    bounded_children: Leaf::new(None),
                    unbounded_child: Atomic::null(),
                }
            },
            floor,
        }
    }

    /// Inserts a key-value pair.
    ///
    /// It is a recursive call, and therefore stack-overflow may occur.
    pub fn insert(&self, mut key: K, value: V, guard: &Guard) -> Option<((K, V), bool)> {
        match &self.entry {
            NodeType::InternalNode {
                bounded_children,
                unbounded_child,
            } => {
                // firstly, try to insert into an existing child node
                if let Some((_, child)) = bounded_children.min_ge(&key) {
                    return unsafe { child.load(Acquire, guard).deref().insert(key, value, guard) };
                }

                // secondly, try to create a new child node
                let mut new_child_node = Atomic::null();
                if !bounded_children.full() {
                    new_child_node = Atomic::new(Node::new(self.floor - 1));
                    let inserted = bounded_children.insert(
                        key.clone(),
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
                        if let Some((_, child)) = bounded_children.min_ge(&key) {
                            drop(unsafe { new_child_node.into_owned() });
                            return unsafe {
                                child.load(Acquire, guard).deref().insert(key, value, guard)
                            };
                        }
                        // try to reuse the newly allocated child node as the tail entry
                        let ((key_returned, child_node_returned), duplicated) = inserted.unwrap();
                        key = key_returned;
                        new_child_node = child_node_returned;
                    }
                }

                // lastly, try to insert into the tail entry
                let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                if current_tail_node.is_null() {
                    if new_child_node.load(Relaxed, guard).is_null() {
                        new_child_node = Atomic::new(Node::new(self.floor - 1));
                    }
                    if let Err(result) = unbounded_child.compare_and_set(
                        current_tail_node,
                        new_child_node.load(Relaxed, guard),
                        Relaxed,
                        &guard,
                    ) {
                        drop(unsafe { new_child_node.into_owned() });
                        current_tail_node = result.current;
                    }
                } else if !new_child_node.load(Relaxed, guard).is_null() {
                    drop(unsafe { new_child_node.into_owned() });
                }
                return unsafe { current_tail_node.deref().insert(key, value, guard) };
            }
            NodeType::LeafNode {
                entry_array,
                side_link,
            } => {
                let array_ptr = entry_array.load(Acquire, guard);
                unsafe { array_ptr.deref() }.insert(key, value, false)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
}
