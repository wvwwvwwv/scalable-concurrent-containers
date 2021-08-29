#[cfg(test)]
mod ebr_model {
    use loom::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    #[test]
    #[ignore]
    fn ebr() {
        struct ModelCollector {
            announcement: AtomicU64,
        }

        impl Default for ModelCollector {
            fn default() -> Self {
                ModelCollector {
                    announcement: AtomicU64::new(1),
                }
            }
        }

        #[derive(Default)]
        struct ModelPointer {
            unreachable: AtomicBool,
            reclaimed: AtomicBool,
        }

        let mut model = loom::model::Builder::new();
        model.max_threads = 2;
        model.check(move || {
            let epoch: Arc<AtomicU64> = Arc::default();
            let collectors: Arc<(ModelCollector, ModelCollector)> = Arc::default();
            let ptr: Arc<ModelPointer> = Arc::default();

            let thread = {
                let (epoch, collectors, ptr) = (epoch.clone(), collectors.clone(), ptr.clone());
                loom::thread::spawn(move || {
                    let _epoch_ref = epoch.as_ref();
                    let _collector_ref = &collectors.0;
                    let _ptr_ref = ptr.as_ref();
                })
            };

            let _epoch_ref = epoch.as_ref();
            let _collector_ref = &collectors.1;
            let _ptr_ref = ptr.as_ref();

            drop(thread.join());
        });
    }
}
