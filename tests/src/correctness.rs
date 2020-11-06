#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use scc::HashMap;
    use std::collections::hash_map::RandomState;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    proptest! {
        #[test]
        fn basic_hashmap(key in 0u64..10) {
            let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(RandomState::new(), Some(10));
            let result1 = hashmap.insert(key, 0);
            assert!(result1.is_ok());
            if let Ok(result) = result1 {
                assert_eq!(result.get(), (&key, &mut 0));
            }

            let result2 = hashmap.insert(key, 0);
            assert!(result2.is_err());
            if let Err((result, _)) = result2 {
                assert_eq!(result.get(), (&key, &mut 0));
            }

            let result3 = hashmap.upsert(key, 1);
            assert_eq!(result3.get(), (&key, &mut 1));
            drop(result3);

            let result4 = hashmap.insert(key, 10);
            assert!(result4.is_err());
            if let Err((result, _)) = result4 {
                assert_eq!(result.get(), (&key, &mut 1));
                *result.get().1 = 2;
            }

            let mut result5 = hashmap.iter();
            assert_eq!(result5.next(), Some((&key, &mut 2)));
            assert_eq!(result5.next(), None);

            for iter in hashmap.iter() {
                assert_eq!(iter, (&key, &mut 2));
                *iter.1 = 3;
            }

            let result6 = hashmap.get(key);
            assert_eq!(result6.unwrap().get(), (&key, &mut 3));

            let result7 = hashmap.get(key + 1);
            assert!(result7.is_none());

            let result8 = hashmap.remove(key);
            assert_eq!(result8, true);

            let result9 = hashmap.insert(key + 2, 10);
            assert!(result9.is_ok());
            if let Ok(result) = result9 {
                assert_eq!(result.get(), (&(key + 2), &mut 10));
                result.erase();
            }

            let result10 = hashmap.get(key + 2);
            assert!(result10.is_none());
        }
    }

    struct Data<'a> {
        data: u64,
        checker: &'a AtomicUsize,
    }

    impl<'a> Data<'a> {
        fn new(data: u64, checker: &'a AtomicUsize) -> Data<'a> {
            checker.fetch_add(1, Relaxed);
            Data {
                data: data,
                checker: checker,
            }
        }
    }

    impl<'a> Clone for Data<'a> {
        fn clone(&self) -> Self {
            Data::new(self.data, self.checker)
        }
    }

    impl<'a> Drop for Data<'a> {
        fn drop(&mut self) {
            self.checker.fetch_sub(1, Relaxed);
        }
    }

    impl<'a> Eq for Data<'a> {}

    impl<'a> Hash for Data<'a> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.data.hash(state);
        }
    }

    impl<'a> PartialEq for Data<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.data == other.data
        }
    }

    proptest! {
        #[test]
        fn insert(key in 0u64..64) {
            let range = 4096;
            let mut checker = AtomicUsize::new(0);
            let hashmap: HashMap<Data, Data, RandomState> = HashMap::new(RandomState::new(), Some(10));
            for d in key..(key + 4096) {
                hashmap.insert(Data::new(d, &checker), Data::new(d, &checker));
                let result = hashmap.upsert(Data::new(d, &checker), Data::new(d + 1, &checker));
                (*result.get().1) = Data::new(d + 2, &checker);
            }
            assert_eq!(checker.load(Relaxed), range * 2);
            for d in key..(key + 4096) {
                let result = hashmap.get(Data::new(d, &checker));
                result.unwrap().erase();
            }
            assert_eq!(checker.load(Relaxed), 0);
        }
    }
}
