use scc::HashMap;
use proptest::prelude::*;

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
        let result1 = hashmap.insert(1, 0);
        assert!(result1.is_err());
        if let Err(result) = result1 {
            assert_eq!(*result.get().unwrap(), (1, 0));
        }
    }

    proptest! {
        #[test]
        fn insert(key in 0u64..1048576u64) {
        }
    }
}
