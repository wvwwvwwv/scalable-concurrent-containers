#[cfg(feature = "loom")]
#[cfg(test)]
mod test_model {
    use crate::TreeIndex;
    use loom::model::Builder;
    use loom::thread::{spawn, yield_now};
    use sdd::Guard;
    use std::borrow::Borrow;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    #[derive(Debug)]
    struct A(usize, Arc<AtomicUsize>);
    impl A {
        fn new(d: usize, c: Arc<AtomicUsize>) -> Self {
            c.fetch_add(1, Relaxed);
            Self(d, c)
        }
    }
    impl Clone for A {
        fn clone(&self) -> Self {
            self.1.fetch_add(1, Relaxed);
            Self(self.0, self.1.clone())
        }
    }
    impl Drop for A {
        fn drop(&mut self) {
            self.1.fetch_sub(1, Relaxed);
        }
    }

    #[test]
    fn tree_index() {
        let mut model_builder = Builder::new();
        model_builder.max_branches = 65536;

        model_builder.check(|| {
            let cnt = Arc::new(AtomicUsize::new(0));
            let tree_index = Arc::new(TreeIndex::<usize, A>::default());

            for k in 0..14 {
                assert!(tree_index.insert(k, A::new(k, cnt.clone())).is_ok());
            }

            let cnt_clone = cnt.clone();
            let tree_index_clone = tree_index.clone();
            let thread = spawn(move || {
                assert!(tree_index_clone.insert(15, A::new(15, cnt_clone)).is_ok());
            });

            for key in [4, 8] {
                assert_eq!(
                    tree_index
                        .peek_with(key.borrow(), |_key, value| value.0)
                        .unwrap(),
                    key
                );
                // assert!(tree_index.remove(key.borrow()));
                // assert!(tree_index.peek_with(key.borrow(), |_key, value| value.0).is_none());
            }

            assert!(thread.join().is_ok());
            drop(tree_index);

            while cnt.load(Relaxed) != 0 {
                Guard::new().accelerate();
                yield_now();
            }
        });
    }
}
