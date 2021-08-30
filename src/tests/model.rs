#[cfg(test)]
mod ebr_model {
    use loom::sync::atomic::fence;
    use loom::sync::atomic::{AtomicBool, AtomicU8};

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};
    use std::sync::Arc;

    struct ModelCollector {
        announcement: AtomicU8,
        epoch_witnessed: AtomicUsize,
        collected: [AtomicUsize; 3],
    }

    impl ModelCollector {
        fn new_barrier(&self, epoch: &AtomicU8, ptr: &ModelPointer) {
            let known_epoch = self.announcement.load(Relaxed) - 1;
            let epoch = epoch.load(Relaxed);
            self.announcement.store(epoch, Relaxed);
            fence(SeqCst);
            if epoch != known_epoch {
                self.epoch_updated(ptr);
            }
        }

        fn epoch_updated(&self, ptr: &ModelPointer) {
            self.epoch_witnessed.fetch_add(1, Relaxed);
            if self.collected[self.epoch_witnessed.load(Relaxed) % 3].swap(0, Relaxed) > 0 {
                assert!(ptr.unreachable.load(Relaxed));
                assert!(!ptr.reclaimed.swap(true, Relaxed));
            }
        }

        fn collect(&self) {
            self.collected[self.epoch_witnessed.load(Relaxed) % 3].fetch_add(1, Relaxed);
        }

        fn end_barrier(&self, epoch: &AtomicU8, ptr: &ModelPointer, other: &ModelCollector) {
            let known_epoch = self.announcement.load(Relaxed);
            let other_epoch = other.announcement.load(Relaxed);
            if other_epoch % 2 == 1 || other_epoch == known_epoch {
                let new = match known_epoch {
                    0 => 2,
                    2 => 4,
                    _ => 0,
                };
                epoch.store(new, Relaxed);
                fence(SeqCst);
                self.announcement.store(new, Relaxed);
                self.epoch_updated(ptr);
            }
            fence(Release);
            self.announcement.store(known_epoch + 1, Relaxed);
        }
    }

    impl Default for ModelCollector {
        fn default() -> Self {
            ModelCollector {
                announcement: AtomicU8::new(1),
                epoch_witnessed: AtomicUsize::new(0),
                collected: Default::default(),
            }
        }
    }

    #[derive(Default)]
    struct ModelPointer {
        unreachable: AtomicBool,
        reclaimed: AtomicBool,
    }

    /// This model requires `https://github.com/tokio-rs/loom/pull/220`.
    #[test]
    #[ignore]
    fn ebr() {
        let mut model = loom::model::Builder::new();
        model.max_threads = 2;
        model.check(move || {
            let epoch: Arc<AtomicU8> = Arc::default();
            let collectors: Arc<(ModelCollector, ModelCollector)> = Arc::default();
            let ptr: Arc<ModelPointer> = Arc::default();

            let thread = {
                let (epoch, collectors, ptr) = (epoch.clone(), collectors.clone(), ptr.clone());
                loom::thread::spawn(move || {
                    let epoch_ref = epoch.as_ref();
                    let collector_ref = &collectors.0;
                    let ptr_ref = ptr.as_ref();

                    collector_ref.new_barrier(epoch_ref, ptr_ref);
                    ptr_ref.unreachable.swap(true, Relaxed);
                    collector_ref.collect();
                    collector_ref.end_barrier(epoch_ref, ptr_ref, &collectors.1);

                    (0..3).for_each(|_| {
                        collector_ref.new_barrier(epoch_ref, ptr_ref);
                        collector_ref.end_barrier(epoch_ref, ptr_ref, &collectors.1);
                    });
                })
            };

            let epoch_ref = epoch.as_ref();
            let collector_ref = &collectors.1;
            let ptr_ref = ptr.as_ref();

            collector_ref.new_barrier(epoch_ref, ptr_ref);
            assert!(ptr_ref.unreachable.load(Relaxed) || !ptr_ref.reclaimed.load(Relaxed));
            collector_ref.end_barrier(epoch_ref, ptr_ref, &collectors.0);

            drop(thread.join());
        });
    }
}
