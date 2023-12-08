#[cfg(test)]
mod examples {
    use scc::HashMap;

    #[test]
    fn single_threaded() {
        let workload_size = 128;
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
}
