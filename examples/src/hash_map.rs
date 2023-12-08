#[cfg(test)]
mod examples {
    use scc::HashMap;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn single_threaded() {
        let workload_size = 256;
        let hashmap: HashMap<isize, isize> = HashMap::new();
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(hashmap.insert(-i, i).is_ok());
            } else {
                assert!(hashmap.insert(i, i).is_ok());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(hashmap.get(&i).is_none());
                assert!(hashmap.get(&-i).is_some());
            } else {
                assert!(hashmap.get(&i).is_some());
                assert!(hashmap.get(&-i).is_none());
            }
        }
        for i in 1..workload_size {
            if i % 2 == 0 {
                assert!(hashmap.remove(&i).is_none());
                assert!(hashmap.remove(&-i).is_some());
                assert!(hashmap.remove(&-i).is_none());
            } else {
                assert!(hashmap.remove(&-i).is_none());
                assert!(hashmap.remove(&i).is_some());
                assert!(hashmap.remove(&i).is_none());
            }
        }
        assert!(hashmap.is_empty());
    }

    #[test]
    fn multi_threaded() {
        let workload_size = 256;
        let hashmap: Arc<HashMap<isize, isize>> = Arc::default();

        thread::scope(|s| {
            s.spawn(|| {
                for i in 1..workload_size {
                    assert!(hashmap.insert(i, i).is_ok());
                }
                assert!(hashmap.get(&0).is_none());
                for i in 1..workload_size {
                    assert!(hashmap.get(&i).is_some());
                }
                for i in 1..workload_size {
                    assert!(hashmap.remove(&i).is_some());
                }
            });
            s.spawn(|| {
                for i in 1..workload_size {
                    assert!(hashmap.insert(-i, i).is_ok());
                }
                assert!(hashmap.get(&0).is_none());
                for i in 1..workload_size {
                    assert!(hashmap.get(&-i).is_some());
                }
                for i in 1..workload_size {
                    assert!(hashmap.remove(&-i).is_some());
                }
            });
        });

        assert!(hashmap.is_empty());
    }
}
