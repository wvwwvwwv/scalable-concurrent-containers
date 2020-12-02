use super::Leaf;
use crossbeam_epoch::Atomic;

enum NodeType<K: Clone + Ord + Sync, V: Clone + Sync> {
    InternalNode(Leaf<K, Node<K, V>>),
    LeafNode(Leaf<K, V>),
}
pub struct Node<K: Clone + Ord + Sync, V: Clone + Sync> {
    entry: Box<NodeType<K, V>>,
}

impl<K: Clone + Ord + Sync, V: Clone + Sync> Clone for Node<K, V> {
    fn clone(&self) -> Self {
        let mut node = Node {
            entry: Box::new(NodeType::LeafNode(Leaf::new(None, Atomic::null()))),
        };
        // do something...
        node
    }
}

unsafe impl<K: Clone + Ord + Sync, V: Clone + Sync> Sync for Node<K, V> {}
