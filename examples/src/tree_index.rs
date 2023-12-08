#[cfg(test)]
mod examples {
    use scc::TreeIndex;
    use std::sync::Arc;
    use std::thread;

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

    #[test]
    fn multi_threaded() {
        let workload_size = 256;
        let treeindex: Arc<TreeIndex<isize, isize>> = Arc::default();

        thread::scope(|s| {
            s.spawn(|| {
                for i in 1..workload_size {
                    assert!(treeindex.insert(i, i).is_ok());
                }
                assert!(treeindex.peek_with(&0, |_, _| ()).is_none());
                for i in 1..workload_size {
                    assert!(treeindex.peek_with(&i, |_, _| ()).is_some());
                }
                for i in 1..workload_size {
                    assert!(treeindex.remove(&i));
                }
            });
            s.spawn(|| {
                for i in 1..workload_size {
                    assert!(treeindex.insert(-i, i).is_ok());
                }
                assert!(treeindex.peek_with(&0, |_, _| ()).is_none());
                for i in 1..workload_size {
                    assert!(treeindex.peek_with(&-i, |_, _| ()).is_some());
                }
                for i in 1..workload_size {
                    assert!(treeindex.remove(&-i));
                }
            });
        });

        assert!(treeindex.is_empty());
    }
}
