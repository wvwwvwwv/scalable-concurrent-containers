#[cfg(test)]
mod ebr_model {
    use loom::sync::atomic::fence;
    use loom::sync::atomic::{AtomicBool, AtomicU64};

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::sync::Arc;

    struct ModelCollector {
        announcement: AtomicU64,
        epoch_witnessed: AtomicUsize,
        collected: [AtomicUsize; 3],
    }

    impl ModelCollector {
        fn new_barrier(&self, epoch: &AtomicU64, ptr: &ModelPointer) {
            let announcement = self.announcement.load(Relaxed) - 1;
            self.announcement.store(announcement, Relaxed);
            fence(SeqCst);
            let epoch = epoch.load(Relaxed);
            fence(SeqCst);
            if epoch != announcement {
                self.epoch_updated(epoch, ptr);
            }
        }

        fn epoch_updated(&self, epoch: u64, ptr: &ModelPointer) {
            self.epoch_witnessed.fetch_add(1, Relaxed);
            self.announcement.store(epoch, Relaxed);
            if self.collected[self.epoch_witnessed.load(Relaxed) % 3].swap(0, Relaxed) > 0 {
                assert!(ptr.unreachable.load(Relaxed));
                assert!(!ptr.reclaimed.swap(true, Relaxed));
            }
        }

        fn collect(&self) {
            self.collected[self.epoch_witnessed.load(Relaxed) % 3].fetch_add(1, Relaxed);
        }

        fn end_barrier(&self, epoch: &AtomicU64, ptr: &ModelPointer, other: &ModelCollector) {
            let announcement = self.announcement.load(Relaxed);
            fence(SeqCst);
            let announcement_other = other.announcement.load(Relaxed);
            if announcement_other % 2 == 1 || announcement_other == announcement {
                fence(SeqCst);
                let epoch = epoch.fetch_add(1, Relaxed) + 1;
                fence(SeqCst);
                self.epoch_updated(epoch, ptr);
                self.announcement.store(epoch + 1, Relaxed);
            } else {
                self.announcement.store(announcement + 1, Relaxed);
            }
            fence(SeqCst);
        }
    }

    impl Default for ModelCollector {
        fn default() -> Self {
            ModelCollector {
                announcement: AtomicU64::new(1),
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
            let epoch: Arc<AtomicU64> = Arc::default();
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
