use super::Leaf;
use crossbeam_epoch::Atomic;

enum NodeType<K: Clone + Ord + Send + Sync, V: Clone + Send + Sync> {
    InternalNode(Leaf<K, Atomic<Node<K, V>>>),
    LeafNode(Leaf<K, Atomic<Leaf<K, V>>>),
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
                NodeType::InternalNode(Leaf::new(None, Atomic::null()))
            },
            floor,
        }
    }
}
