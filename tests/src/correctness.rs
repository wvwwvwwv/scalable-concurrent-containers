use proptest::prelude::*;
use scc::HashMap;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::RandomState;

    #[test]
    fn basic_hashmap() {
        let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(RandomState::new(), Some(10));
        let result1 = hashmap.insert(1, 0);
        assert!(result1.is_ok());
        if let Ok(result) = result1 {
            assert_eq!(*result.get().unwrap(), (1, 0));
        }
        let result2 = hashmap.insert(1, 0);
        assert!(result2.is_err());
        if let Err((result, _)) = result2 {
            assert_eq!(*result.get().unwrap(), (1, 0));
        }
        let result3 = hashmap.upsert(1, 1);
        assert_eq!(*result3.get().unwrap(), (1, 1));
        drop(result3);

        let result4 = hashmap.insert(1, 10);
        assert!(result4.is_err());
        if let Err((result, _)) = result4 {
            assert_eq!(*result.get().unwrap(), (1, 1));
        }

        let result5 = hashmap.get(1);
        assert_eq!(*result5.get().unwrap(), (1, 1));
        drop(result5);

        let result6 = hashmap.get(2);
        assert_eq!(result6.get(), None);

        let result7 = hashmap.remove(1);
        assert_eq!(result7, true);

        let result8 = hashmap.insert(10, 10);
        assert!(result8.is_ok());
        if let Ok(result) = result8 {
            assert_eq!(*result.get().unwrap(), (10, 10));
            result.erase();
        }

        let result9 = hashmap.get(10);
        assert_eq!(result9.get(), None);
    }

    proptest! {
        #[test]
        fn insert(key in 0u64..1048576u64) {
        }
    }
}
