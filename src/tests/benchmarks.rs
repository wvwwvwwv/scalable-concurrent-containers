#![allow(clippy::inline_always)]

mod common {
    use std::hash::{BuildHasher, Hash};
    use std::ptr::addr_of;

    use sdd::Guard;

    use crate::{HashIndex, HashMap, TreeIndex};

    #[allow(clippy::struct_excessive_bools)]
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Workload {
        pub size: usize,
        pub insert_local: bool,
        pub insert_remote: bool,
        pub scan: bool,
        pub read_local: bool,
        pub read_remote: bool,
        pub remove_local: bool,
        pub remove_remote: bool,
    }

    impl Workload {
        pub fn new(workload_size: usize) -> Self {
            let default = Self::default();
            Self {
                size: workload_size,
                ..default
            }
        }
        pub fn insert_local(self) -> Self {
            Self {
                insert_local: true,
                ..self
            }
        }
        pub fn insert_remote(self) -> Self {
            Self {
                insert_remote: true,
                ..self
            }
        }
        pub fn scan(self) -> Self {
            Self { scan: true, ..self }
        }
        pub fn read_local(self) -> Self {
            Self {
                read_local: true,
                ..self
            }
        }
        pub fn read_remote(self) -> Self {
            Self {
                read_remote: true,
                ..self
            }
        }
        pub fn remove_local(self) -> Self {
            Self {
                remove_local: true,
                ..self
            }
        }

        pub fn remove_remote(self) -> Self {
            Self {
                remove_remote: true,
                ..self
            }
        }

        pub fn has_remote_op(&self) -> bool {
            self.insert_remote || self.read_remote || self.remove_remote
        }
    }

    pub trait BenchmarkOperation<K, V, H>: 'static + Send + Sync
    where
        K: 'static + Clone + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + Send + Sync,
        H: 'static + BuildHasher + Send + Sync,
    {
        fn insert_async(&self, k: K, v: V) -> impl Future<Output = bool> + Send;
        fn read_async(&self, k: &K) -> impl Future<Output = bool> + Send;
        fn scan_async(&self) -> impl Future<Output = usize> + Send;
        fn remove_async(&self, k: &K) -> impl Future<Output = bool> + Send;
        fn insert_sync(&self, k: K, v: V) -> bool;
        fn read_sync(&self, k: &K) -> bool;
        fn scan_sync(&self) -> usize;
        fn remove_sync(&self, k: &K) -> bool;
    }

    impl<K, V, H> BenchmarkOperation<K, V, H> for HashMap<K, V, H>
    where
        K: 'static + Clone + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + Send + Sync,
        H: 'static + BuildHasher + Send + Sync,
    {
        #[inline(always)]
        async fn insert_async(&self, k: K, v: V) -> bool {
            self.insert_async(k, v).await.is_ok()
        }
        #[inline(always)]
        async fn read_async(&self, k: &K) -> bool {
            self.read_async(k, |_, _| ()).await.is_some()
        }
        #[inline(always)]
        async fn scan_async(&self) -> usize {
            let mut scanned = 0;
            self.iter_async(|_, v| {
                if addr_of!(v) as usize != 0 {
                    scanned += 1;
                }
                true
            })
            .await;
            scanned
        }
        #[inline(always)]
        async fn remove_async(&self, k: &K) -> bool {
            self.remove_async(k).await.is_some()
        }
        #[inline(always)]
        fn insert_sync(&self, k: K, v: V) -> bool {
            self.insert_sync(k, v).is_ok()
        }
        #[inline(always)]
        fn read_sync(&self, k: &K) -> bool {
            self.read_sync(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_sync(&self) -> usize {
            let mut scanned = 0;
            self.iter_sync(|_, v| {
                if addr_of!(v) as usize != 0 {
                    scanned += 1;
                }
                true
            });
            scanned
        }

        #[inline(always)]
        fn remove_sync(&self, k: &K) -> bool {
            self.remove_sync(k).is_some()
        }
    }

    impl<K, V, H> BenchmarkOperation<K, V, H> for HashIndex<K, V, H>
    where
        K: 'static + Clone + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + Send + Sync,
        H: 'static + BuildHasher + Send + Sync,
    {
        #[inline(always)]
        async fn insert_async(&self, k: K, v: V) -> bool {
            self.insert_async(k, v).await.is_ok()
        }
        #[inline(always)]
        async fn read_async(&self, k: &K) -> bool {
            self.peek_with(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        async fn scan_async(&self) -> usize {
            let mut scanned = 0;
            self.iter_async(|_, v| {
                if addr_of!(v) as usize != 0 {
                    scanned += 1;
                }
                true
            })
            .await;
            scanned
        }
        #[inline(always)]
        async fn remove_async(&self, k: &K) -> bool {
            self.remove_async(k).await
        }
        #[inline(always)]
        fn insert_sync(&self, k: K, v: V) -> bool {
            self.insert_sync(k, v).is_ok()
        }
        #[inline(always)]
        fn read_sync(&self, k: &K) -> bool {
            self.peek_with(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_sync(&self) -> usize {
            let guard = Guard::new();
            self.iter(&guard)
                .filter(|(_, v)| addr_of!(v) as usize != 0)
                .count()
        }
        #[inline(always)]
        fn remove_sync(&self, k: &K) -> bool {
            self.remove_sync(k)
        }
    }

    impl<K, V, H> BenchmarkOperation<K, V, H> for TreeIndex<K, V>
    where
        K: 'static + Clone + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + Send + Sync,
        H: 'static + BuildHasher + Send + Sync,
    {
        #[inline(always)]
        async fn insert_async(&self, k: K, v: V) -> bool {
            self.insert_async(k, v).await.is_ok()
        }
        #[inline(always)]
        async fn read_async(&self, k: &K) -> bool {
            self.peek_with(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        async fn scan_async(&self) -> usize {
            let guard = Guard::new();
            self.iter(&guard)
                .filter(|(_, v)| addr_of!(v) as usize != 0)
                .count()
        }
        #[inline(always)]
        async fn remove_async(&self, k: &K) -> bool {
            self.remove_async(k).await
        }
        #[inline(always)]
        fn insert_sync(&self, k: K, v: V) -> bool {
            self.insert_sync(k, v).is_ok()
        }
        #[inline(always)]
        fn read_sync(&self, k: &K) -> bool {
            self.peek_with(k, |_, _| ()).is_some()
        }
        #[inline(always)]
        fn scan_sync(&self) -> usize {
            let guard = Guard::new();
            self.iter(&guard)
                .filter(|(_, v)| addr_of!(v) as usize != 0)
                .count()
        }
        #[inline(always)]
        fn remove_sync(&self, k: &K) -> bool {
            self.remove_sync(k)
        }
    }

    pub trait ConvertFromUsize {
        fn convert(from: usize) -> Self;
    }

    impl ConvertFromUsize for usize {
        #[inline(always)]
        fn convert(from: usize) -> usize {
            from
        }
    }

    impl ConvertFromUsize for String {
        #[inline(always)]
        fn convert(from: usize) -> String {
            from.to_string()
        }
    }
}

mod async_benchmarks {
    use std::collections::hash_map::RandomState;
    use std::hash::Hash;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::{Duration, Instant};

    use tokio::sync::Barrier;

    use super::common::{BenchmarkOperation, ConvertFromUsize, Workload};
    use crate::{HashIndex, HashMap, TreeIndex};

    async fn perform<K, V, C>(
        num_tasks: usize,
        start_index: usize,
        container: &Arc<C>,
        workload: Workload,
    ) -> (Duration, usize)
    where
        K: 'static + Clone + ConvertFromUsize + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + ConvertFromUsize + Send + Sync,
        C: 'static + BenchmarkOperation<K, V, RandomState>,
    {
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut task_handles = Vec::with_capacity(num_tasks);
        let barrier = Arc::new(Barrier::new(num_tasks + 1));
        for task_id in 0..num_tasks {
            let barrier = barrier.clone();
            let container = container.clone();
            let total_num_operations = total_num_operations.clone();
            task_handles.push(tokio::task::spawn(async move {
                let mut num_operations = 0;
                barrier.wait().await;
                if workload.scan {
                    num_operations += container.scan_async().await;
                }
                for i in 0..workload.size {
                    let remote_task_id = if num_tasks < 2 {
                        0
                    } else {
                        (task_id + 1 + i % (num_tasks - 1)) % num_tasks
                    };
                    assert!(num_tasks < 2 || task_id != remote_task_id);
                    if workload.insert_local {
                        let local_index = task_id * workload.size + i + start_index;
                        let result = container
                            .insert_async(K::convert(local_index), V::convert(i))
                            .await;
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.insert_remote {
                        let remote_index = remote_task_id * workload.size + i + start_index;
                        container
                            .insert_async(K::convert(remote_index), V::convert(i))
                            .await;
                        num_operations += 1;
                    }
                    if workload.read_local {
                        let local_index = task_id * workload.size + i + start_index;
                        let result = container.read_async(&K::convert(local_index)).await;
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.read_remote {
                        let remote_index = remote_task_id * workload.size + i + start_index;
                        container.read_async(&K::convert(remote_index)).await;
                        num_operations += 1;
                    }
                    if workload.remove_local {
                        let local_index = task_id * workload.size + i + start_index;
                        let result = container.remove_async(&K::convert(local_index)).await;
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.remove_remote {
                        let remote_index = remote_task_id * workload.size + i + start_index;
                        container.remove_async(&K::convert(remote_index)).await;
                        num_operations += 1;
                    }
                }
                barrier.wait().await;
                total_num_operations.fetch_add(num_operations, Relaxed);
            }));
        }

        barrier.wait().await;
        let start_time = Instant::now();
        barrier.wait().await;
        let end_time = Instant::now();

        for r in futures::future::join_all(task_handles).await {
            assert!(r.is_ok());
        }

        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    async fn hashmap_benchmark(workload_size: usize, num_tasks_list: Vec<usize>) {
        for num_tasks in num_tasks_list {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> = Arc::new(HashMap::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, insert).await;
            println!("hashmap-insert-local: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, scan).await;
            println!("hashmap-scan: {num_tasks}, {duration:?}, {total_num_operations}");

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, read).await;
            println!("hashmap-read-local: {num_tasks}, {duration:?}, {total_num_operations}");

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, remove).await;
            println!("hashmap-remove-local: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, insert).await;
            println!(
                "hashmap-insert-local-remote: {num_tasks}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_tasks, workload_size * num_tasks, &hashmap, mixed).await;
            println!("hashmap-mixed: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), workload_size * num_tasks);

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().remove_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashmap, remove).await;
            println!(
                "hashmap-remove-local-remote: {num_tasks}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashmap.len(), 0);
        }
    }

    async fn hashindex_benchmark(workload_size: usize, num_tasks_list: Vec<usize>) {
        for num_tasks in num_tasks_list {
            let hashindex: Arc<HashIndex<usize, usize, RandomState>> =
                Arc::new(HashIndex::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, insert).await;
            println!("hashindex-insert-local: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), workload_size * num_tasks);

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, scan).await;
            println!("hashindex-scan: {num_tasks}, {duration:?}, {total_num_operations}");

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, read).await;
            println!("hashindex-read-local: {num_tasks}, {duration:?}, {total_num_operations}");

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, remove).await;
            println!("hashindex-remove-local: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, insert).await;
            println!(
                "hashindex-insert-local-remote: {num_tasks}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashindex.len(), workload_size * num_tasks);

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_tasks, workload_size * num_tasks, &hashindex, mixed).await;
            println!("hashindex-mixed: {num_tasks}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), workload_size * num_tasks);

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().remove_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &hashindex, remove).await;
            println!(
                "hashindex-remove-local-remote: {num_tasks}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashindex.len(), 0);
        }
    }

    async fn treeindex_benchmark(workload_size: usize, num_tasks_list: Vec<usize>) {
        for num_tasks in num_tasks_list {
            let treeindex: Arc<TreeIndex<usize, usize>> = Arc::new(TreeIndex::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, insert).await;
            println!(
                "treeindex-insert-local: {}, {:?}, {}, depth = {}",
                num_tasks,
                duration,
                total_num_operations,
                treeindex.depth()
            );
            assert_eq!(treeindex.len(), workload_size * num_tasks);

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, scan).await;
            println!("treeindex-scan: {num_tasks}, {duration:?}, {total_num_operations}",);

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, read).await;
            println!("treeindex-read-local: {num_tasks}, {duration:?}, {total_num_operations}",);

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, remove).await;
            println!("treeindex-remove-local: {num_tasks}, {duration:?}, {total_num_operations}",);
            assert_eq!(treeindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, insert).await;
            println!(
                "treeindex-insert-local-remote: {}, {:?}, {}, depth = {}",
                num_tasks,
                duration,
                total_num_operations,
                treeindex.depth()
            );
            assert_eq!(treeindex.len(), workload_size * num_tasks);

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_tasks, workload_size * num_tasks, &treeindex, mixed).await;
            println!("treeindex-mixed: {num_tasks}, {duration:?}, {total_num_operations}",);
            assert_eq!(treeindex.len(), workload_size * num_tasks);

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().remove_remote();
            let (duration, total_num_operations) = perform(num_tasks, 0, &treeindex, remove).await;
            println!(
                "treeindex-remove-local-remote: {num_tasks}, {duration:?}, {total_num_operations}",
            );
            assert_eq!(treeindex.len(), 0);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn benchmarks_async() {
        hashmap_benchmark(65536, vec![4, 8]).await;
        hashindex_benchmark(65536, vec![4, 8]).await;
        treeindex_benchmark(65536, vec![4, 8]).await;
    }

    #[ignore = "too long"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 128)]
    async fn full_scale_benchmarks_async() {
        hashmap_benchmark(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        )
        .await;
        println!("----");
        hashindex_benchmark(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        )
        .await;
        println!("----");
        treeindex_benchmark(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        )
        .await;
    }
}

mod sync_benchmarks {
    use std::collections::hash_map::RandomState;
    use std::hash::Hash;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use sdd::Guard;

    use super::common::{BenchmarkOperation, ConvertFromUsize, Workload};
    use crate::{HashIndex, HashMap, TreeIndex};

    fn perform<K, V, C>(
        num_threads: usize,
        start_index: usize,
        container: &Arc<C>,
        workload: Workload,
    ) -> (Duration, usize)
    where
        K: 'static + Clone + ConvertFromUsize + Eq + Hash + Ord + Send + Sync,
        V: 'static + Clone + ConvertFromUsize + Send + Sync,
        C: BenchmarkOperation<K, V, RandomState> + 'static + Send + Sync,
    {
        for _ in 0..64 {
            Guard::new().accelerate();
        }
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let container = container.clone();
            let barrier = barrier.clone();
            let total_num_operations = total_num_operations.clone();
            thread_handles.push(thread::spawn(move || {
                let mut num_operations = 0;
                barrier.wait();
                if workload.scan {
                    num_operations += container.scan_sync();
                }
                for i in 0..workload.size {
                    let remote_thread_id = if num_threads < 2 {
                        0
                    } else {
                        (thread_id + 1 + i % (num_threads - 1)) % num_threads
                    };
                    assert!(num_threads < 2 || thread_id != remote_thread_id);
                    if workload.insert_local {
                        let local_index = thread_id * workload.size + i + start_index;
                        let result = container.insert_sync(K::convert(local_index), V::convert(i));
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.insert_remote {
                        let remote_index = remote_thread_id * workload.size + i + start_index;
                        container.insert_sync(K::convert(remote_index), V::convert(i));
                        num_operations += 1;
                    }
                    if workload.read_local {
                        let local_index = thread_id * workload.size + i + start_index;
                        let result = container.read_sync(&K::convert(local_index));
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.read_remote {
                        let remote_index = remote_thread_id * workload.size + i + start_index;
                        container.read_sync(&K::convert(remote_index));
                        num_operations += 1;
                    }
                    if workload.remove_local {
                        let local_index = thread_id * workload.size + i + start_index;
                        let result = container.remove_sync(&K::convert(local_index));
                        assert!(result || workload.has_remote_op());
                        num_operations += 1;
                    }
                    if workload.remove_remote {
                        let remote_index = remote_thread_id * workload.size + i + start_index;
                        container.remove_sync(&K::convert(remote_index));
                        num_operations += 1;
                    }
                }
                barrier.wait();
                total_num_operations.fetch_add(num_operations, Relaxed);
            }));
        }
        barrier.wait();
        let start_time = Instant::now();
        barrier.wait();
        let end_time = Instant::now();
        for handle in thread_handles {
            handle.join().unwrap();
        }
        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    fn hashmap_benchmark<T>(workload_size: usize, num_threads: Vec<usize>)
    where
        T: 'static + ConvertFromUsize + Clone + Eq + Hash + Ord + Send + Sync,
    {
        for num_threads in num_threads {
            let hashmap: Arc<HashMap<usize, usize, RandomState>> = Arc::new(HashMap::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, insert);
            println!("hashmap-insert-local: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, scan);
            println!("hashmap-scan: {num_threads}, {duration:?}, {total_num_operations}");

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, read);
            println!("hashmap-read-local: {num_threads}, {duration:?}, {total_num_operations}");

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, remove);
            println!("hashmap-remove-local: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, insert);
            println!(
                "hashmap-insert-local-remote: {num_threads}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_threads, workload_size * num_threads, &hashmap, mixed);
            println!("hashmap-mixed: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashmap.len(), workload_size * num_threads);

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().remove_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashmap, remove);
            println!(
                "hashmap-remove-local-remote: {num_threads}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashmap.len(), 0);
        }
    }

    fn hashindex_benchmark<T>(workload_size: usize, num_threads: Vec<usize>)
    where
        T: 'static + ConvertFromUsize + Clone + Eq + Hash + Ord + Send + Sync,
    {
        for num_threads in num_threads {
            let hashindex: Arc<HashIndex<T, T, RandomState>> = Arc::new(HashIndex::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, insert);
            println!("hashindex-insert-local: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, scan);
            println!("hashindex-scan: {num_threads}, {duration:?}, {total_num_operations}");

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, read);
            println!("hashindex-read-local: {num_threads}, {duration:?}, {total_num_operations}");

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, remove);
            println!("hashindex-remove-local: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, insert);
            println!(
                "hashindex-insert-local-remote: {num_threads}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_threads, workload_size * num_threads, &hashindex, mixed);
            println!("hashindex-mixed: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(hashindex.len(), workload_size * num_threads);

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().remove_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &hashindex, remove);
            println!(
                "hashindex-remove-local-remote: {num_threads}, {duration:?}, {total_num_operations}"
            );
            assert_eq!(hashindex.len(), 0);
        }
    }

    fn treeindex_benchmark<T>(workload_size: usize, num_threads: Vec<usize>)
    where
        T: 'static + ConvertFromUsize + Clone + Hash + Ord + Send + Sync,
    {
        for num_threads in num_threads {
            let treeindex: Arc<TreeIndex<T, T>> = Arc::new(TreeIndex::default());

            // 1. insert-local
            let insert = Workload::new(workload_size).insert_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, insert);
            assert_eq!(treeindex.len(), total_num_operations);
            println!(
                "treeindex-insert-local: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.depth()
            );

            // 2. scan
            let scan = Workload::new(workload_size).scan();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, scan);
            println!("treeindex-scan: {num_threads}, {duration:?}, {total_num_operations}");

            // 3. read-local
            let read = Workload::new(workload_size).read_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, read);
            println!("treeindex-read-local: {num_threads}, {duration:?}, {total_num_operations}");

            // 4. remove-local
            let remove = Workload::new(workload_size).remove_local();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, remove);
            println!("treeindex-remove-local: {num_threads}, {duration:?}, {total_num_operations}");
            assert_eq!(treeindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload::new(workload_size).insert_local().insert_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, insert);
            assert_eq!(treeindex.len(), total_num_operations / 2);
            println!(
                "treeindex-insert-local-remote: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.depth()
            );

            // 6. mixed
            let mixed = Workload::new(workload_size)
                .insert_local()
                .insert_remote()
                .read_local()
                .read_remote()
                .remove_local()
                .remove_remote();
            let (duration, total_num_operations) =
                perform(num_threads, treeindex.len(), &treeindex, mixed);
            println!("treeindex-mixed: {num_threads}, {duration:?}, {total_num_operations}");

            // 7. remove-local-remote
            let remove = Workload::new(workload_size).remove_local().read_remote();
            let (duration, total_num_operations) = perform(num_threads, 0, &treeindex, remove);
            println!(
                "treeindex-remove-local-remote: {num_threads}, {duration:?}, {total_num_operations}"
            );
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn benchmarks_sync() {
        hashmap_benchmark::<usize>(65536, vec![4, 8]);
        hashindex_benchmark::<usize>(65536, vec![4, 8]);
        treeindex_benchmark::<usize>(65536, vec![4, 8]);
    }

    #[ignore = "too long"]
    #[test]
    fn full_scale_benchmarks_sync() {
        hashmap_benchmark::<usize>(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        );
        println!("----");
        hashindex_benchmark::<usize>(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        );
        println!("----");
        treeindex_benchmark::<usize>(
            1024 * 1024 * 16,
            vec![1, 1, 1, 4, 4, 4, 16, 16, 16, 64, 64, 64],
        );
    }
}
