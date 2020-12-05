use super::leaf::Scanner;
use super::Leaf;
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::cmp::Ordering;
use std::mem::MaybeUninit;
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
    pub fn new_bounded(floor: usize, key: &K, value: &V) -> Node<K, V> {
        Node {
            entry: if floor == 0 {
                NodeType::LeafNode {
                    entry_array: Atomic::new(Leaf::new(Some((key.clone(), value.clone())))),
                    side_link: Atomic::null(),
                }
            } else {
                NodeType::InternalNode {
                    bounded_children: Leaf::new(Some((
                        key.clone(),
                        Atomic::new(Node::new_bounded(floor - 1, key, value)),
                    ))),
                    unbounded_child: Atomic::null(),
                }
            },
            floor,
        }
    }

    pub fn new_unbounded(floor: usize) -> Node<K, V> {
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
    pub fn insert(
        &self,
        mut key: K,
        value: V,
        guard: &Guard,
    ) -> Result<Option<Atomic<Node<K, V>>>, ((K, V), bool)> {
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
                if !bounded_children.full() {
                    let new_child_node =
                        Atomic::new(Node::new_bounded(self.floor - 1, &key, &value));
                    let inserted = bounded_children.insert(
                        key.clone(),
                        Atomic::from(new_child_node.load(Relaxed, guard)),
                        false,
                    );
                    if inserted.is_none() {
                        return Ok(None);
                    }

                    drop(unsafe { new_child_node.into_owned() });
                    if let Some((_, child)) = bounded_children.min_ge(&key) {
                        return unsafe {
                            child.load(Acquire, guard).deref().insert(key, value, guard)
                        };
                    }
                    key = (inserted.unwrap().0).0;
                }

                // lastly, try to insert into the tail
                let mut current_tail_node = unbounded_child.load(Relaxed, guard);
                if current_tail_node.is_null() {
                    let new_child_node = Atomic::new(Node::new_unbounded(self.floor - 1));
                    if let Err(result) = unbounded_child.compare_and_set(
                        current_tail_node,
                        new_child_node.load(Relaxed, guard),
                        Relaxed,
                        &guard,
                    ) {
                        drop(unsafe { new_child_node.into_owned() });
                        current_tail_node = result.current;
                    }
                }
                return unsafe { current_tail_node.deref().insert(key, value, guard) };
            }
            NodeType::LeafNode {
                entry_array,
                side_link,
            } => {
                let array_ptr = entry_array.load(Acquire, guard);
                match unsafe { array_ptr.deref() }.insert(key, value, false) {
                    Some(result) => Err(result),
                    None => Ok(None),
                }
            }
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
                entry_array,
                side_link,
            } => {
                let tail = entry_array.swap(Shared::null(), Acquire, &guard);
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
        let node = Node::new_unbounded(2);
        assert!(node.insert(10, 10, &guard).is_ok());
        assert!(node.insert(10, 11, &guard).is_err());
    }
}
