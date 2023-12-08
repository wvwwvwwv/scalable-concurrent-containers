#[cfg(test)]
mod examples {
    use scc::HashIndex;

    #[test]
    fn single_threaded() {
        let workload_size = 128;
        let hashindex: HashIndex<isize, isize> = HashIndex::new();
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(hashindex.insert(-i, i).is_ok());
            } else {
                assert!(hashindex.insert(i, i).is_ok());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(hashindex.peek_with(&i, |_, _| ()).is_none());
                assert!(hashindex.peek_with(&-i, |_, _| ()).is_some());
            } else {
                assert!(hashindex.peek_with(&i, |_, _| ()).is_some());
                assert!(hashindex.peek_with(&-i, |_, _| ()).is_none());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(!hashindex.remove(&i));
                assert!(hashindex.remove(&-i));
                assert!(!hashindex.remove(&-i));
            } else {
                assert!(!hashindex.remove(&-i));
                assert!(hashindex.remove(&i));
                assert!(!hashindex.remove(&i));
            }
        }
        assert!(hashindex.is_empty());
    }
}
