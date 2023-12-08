#[cfg(test)]
mod examples {
    use scc::TreeIndex;

    #[test]
    fn single_threaded() {
        let workload_size = 256;
        let treeindex: TreeIndex<isize, isize> = TreeIndex::new();
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(treeindex.insert(-i, i).is_ok());
            } else {
                assert!(treeindex.insert(i, i).is_ok());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(treeindex.peek_with(&i, |_, _| ()).is_none());
                assert!(treeindex.peek_with(&-i, |_, _| ()).is_some());
            } else {
                assert!(treeindex.peek_with(&i, |_, _| ()).is_some());
                assert!(treeindex.peek_with(&-i, |_, _| ()).is_none());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(!treeindex.remove(&i));
                assert!(treeindex.remove(&-i));
                assert!(!treeindex.remove(&-i));
            } else {
                assert!(!treeindex.remove(&-i));
                assert!(treeindex.remove(&i));
                assert!(!treeindex.remove(&i));
            }
        }
        assert!(treeindex.is_empty());
    }
}
