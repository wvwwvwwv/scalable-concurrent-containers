use super::link::{EntryArrayLink, LinkType};
use std::convert::TryInto;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: u8 = 16;
const KILLED_FLAG: u32 = 1 << 31;
const WAITING_FLAG: u32 = 1 << 30;
const LOCK_MASK: u32 = ((1 << 14) - 1) << ARRAY_SIZE;
const XLOCK: u32 = 1 << 29;
const SLOCK_MAX: u32 = LOCK_MASK & (!XLOCK);
const SLOCK: u32 = 1 << ARRAY_SIZE;
const OCCUPANCY_MASK: u32 = (1 << ARRAY_SIZE) - 1;
const OCCUPANCY_BIT: u32 = 1;

pub struct Cell<K: Clone + Eq, V> {
    partial_hash_array: [u16; ARRAY_SIZE as usize],
    metadata: AtomicU32,
    wait_queue: AtomicPtr<WaitQueueEntry>,
    link: LinkType<K, V>,
    linked_entries: usize,
}

impl<K: Clone + Eq, V> Cell<K, V> {
    pub fn killed(&self) -> bool {
        self.metadata.load(Relaxed) & KILLED_FLAG == KILLED_FLAG
    }

    pub fn size(&self) -> (usize, usize) {
        (
            (self.metadata.load(Relaxed) & OCCUPANCY_MASK).count_ones() as usize,
            self.linked_entries,
        )
    }

    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F) -> Option<T> {
        // insert the condvar into the wait queue
        let mut condvar = WaitQueueEntry::new(self.wait_queue.load(Relaxed));
        let condvar_ptr: *mut WaitQueueEntry = &mut condvar;

        // insert itself into the wait queue
        while let Err(result) =
            self.wait_queue
                .compare_exchange(condvar.next, condvar_ptr, Release, Relaxed)
        {
            condvar.next = result;
        }

        // 'Relaxed' is sufficient, because this thread reading the flag state as 'set'
        // while the actual value is 'unset' means that the lock owner has released it.
        let mut current = self.metadata.load(Relaxed);
        while current & WAITING_FLAG == 0 {
            match self
                .metadata
                .compare_exchange(current, current | WAITING_FLAG, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(result) => current = result,
            }
        }

        // try-lock again once the condvar is inserted into the wait queue
        let locked = f();
        if locked.is_some() {
            self.wakeup();
        }
        condvar.wait();
        locked
    }

    fn wakeup(&self) {
        let mut condvar_ptr: *mut WaitQueueEntry = self.wait_queue.load(Acquire);
        while let Err(result) =
            self.wait_queue
                .compare_exchange(condvar_ptr, ptr::null_mut(), Acquire, Relaxed)
        {
            condvar_ptr = result;
            if condvar_ptr == ptr::null_mut() {
                return;
            }
        }

        while condvar_ptr != ptr::null_mut() {
            let next_ptr = unsafe { (*condvar_ptr).next };
            unsafe {
                (*condvar_ptr).signal();
            };
            condvar_ptr = next_ptr;
        }
    }
}

impl<K: Clone + Eq, V> Default for Cell<K, V> {
    fn default() -> Self {
        Cell {
            metadata: AtomicU32::new(0),
            wait_queue: AtomicPtr::new(ptr::null_mut()),
            partial_hash_array: [0; ARRAY_SIZE as usize],
            link: None,
            linked_entries: 0,
        }
    }
}

/// CellLocker
pub struct CellLocker<'a, K: Clone + Eq, V> {
    cell: &'a Cell<K, V>,
    metadata: u32,
}

impl<'a, K: Clone + Eq, V> CellLocker<'a, K, V> {
    /// Create a new CellLocker instance with the cell exclusively locked.
    pub fn lock(cell: &'a Cell<K, V>) -> CellLocker<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Create a new CellLocker instance if the cell is exclusively locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<CellLocker<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            match cell.metadata.compare_exchange(
                current & (!LOCK_MASK),
                (current & (!LOCK_MASK)) | XLOCK,
                Acquire,
                Relaxed,
            ) {
                Ok(result) => {
                    debug_assert_eq!(result & LOCK_MASK, 0);
                    return Some(CellLocker {
                        cell: cell,
                        metadata: result | XLOCK,
                    });
                }
                Err(result) => {
                    if result & LOCK_MASK != 0 {
                        return None;
                    }
                    current = result;
                }
            }
        }
    }

    pub fn occupied(&self, index: u8) -> bool {
        (self.metadata & (OCCUPANCY_BIT << index)) != 0
    }

    pub fn overflowing(&self) -> bool {
        self.cell.linked_entries > 0
    }

    pub fn next_occupied(&self, index: u8) -> u8 {
        let start_index = if index == u8::MAX { 0 } else { index + 1 };
        for i in start_index..ARRAY_SIZE {
            if self.occupied(i) {
                return i;
            }
        }
        u8::MAX
    }

    pub fn search_preferred(&self, partial_hash: u16) -> Option<u8> {
        let preferred_index = (partial_hash % (ARRAY_SIZE as u16)).try_into().unwrap();
        if self.cell.partial_hash_array[preferred_index as usize] == partial_hash
            && self.occupied(preferred_index)
        {
            return Some(preferred_index.try_into().unwrap());
        }
        None
    }

    pub fn search(&self, start_index: u8, partial_hash: u16) -> Option<u8> {
        for i in start_index..ARRAY_SIZE {
            if self.cell.partial_hash_array[i as usize] == partial_hash
                && self.occupied(i.try_into().unwrap())
            {
                return Some(i.try_into().unwrap());
            }
        }
        None
    }

    pub fn insert(&mut self, partial_hash: u16) -> Option<u8> {
        let preferred_index = (partial_hash % (ARRAY_SIZE as u16)).try_into().unwrap();
        let cell_ptr = self.cell as *const Cell<K, V>;
        let cell_mut_ptr = cell_ptr as *mut Cell<K, V>;
        if !self.occupied(preferred_index) {
            self.metadata = self.metadata | (OCCUPANCY_BIT << preferred_index);
            unsafe { (*cell_mut_ptr).partial_hash_array[preferred_index as usize] = partial_hash };
            return Some(preferred_index.try_into().unwrap());
        }
        for (i, _) in self.cell.partial_hash_array.iter().enumerate() {
            if i != (preferred_index as usize) && !self.occupied(i.try_into().unwrap()) {
                self.metadata = self.metadata | (OCCUPANCY_BIT << i);
                unsafe { (*cell_mut_ptr).partial_hash_array[i as usize] = partial_hash };
                return Some(i.try_into().unwrap());
            }
        }
        None
    }

    pub fn remove(&mut self, index: u8) {
        debug_assert!(index < ARRAY_SIZE);
        debug_assert!(self.metadata & (OCCUPANCY_BIT << index) == (OCCUPANCY_BIT << index));
        self.metadata = self.metadata & (!(OCCUPANCY_BIT << index));
    }

    pub fn first_entry(&self) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        self.cell
            .link
            .as_ref()
            .map_or((ptr::null(), ptr::null()), |entry| entry.first_entry())
    }

    pub fn search_link(
        &self,
        key: &K,
        partial_hash: u16,
    ) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        let mut link_ref = &self.cell.link;
        while let Some(link) = link_ref.as_ref() {
            if let Some(result) = link.search_entry(key, partial_hash) {
                return result;
            }
            link_ref = &link.link;
        }
        (ptr::null(), ptr::null())
    }

    pub fn insert_link(
        &mut self,
        key: &K,
        partial_hash: u16,
        value: V,
    ) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        let cell = self.get_cell_mut_ref();
        let mut value = value;
        let mut link_ref = &mut cell.link;
        while let Some(link) = link_ref.as_mut() {
            match link.insert_entry(key, partial_hash, value) {
                Ok(result) => {
                    cell.linked_entries += 1;
                    return result;
                }
                Err(result) => value = result,
            }
            link_ref = &mut link.link;
        }

        let mut new_entry_array_link = Box::new(EntryArrayLink::new(cell.link.take()));
        let result = new_entry_array_link.insert_entry(key, partial_hash, value);
        cell.link = Some(new_entry_array_link);
        cell.linked_entries += 1;
        return result.ok().unwrap();
    }

    pub fn remove_link(
        &mut self,
        entry_array_link_ptr: *const EntryArrayLink<K, V>,
        key_value_pair_ptr: *const (K, V),
    ) {
        let cell = self.get_cell_mut_ref();
        let entry_array_link_mut_ptr = entry_array_link_ptr as *mut EntryArrayLink<K, V>;
        if unsafe { (*entry_array_link_mut_ptr).remove_entry(key_value_pair_ptr) } {
            if let Ok(mut head) = cell
                .link
                .as_mut()
                .map_or(Err(()), |head| head.remove_self(entry_array_link_mut_ptr))
            {
                cell.link = head.take();
            } else {
                let mut link_ref = &mut cell.link;
                while let Some(link) = link_ref.as_mut() {
                    if link.remove_next(entry_array_link_mut_ptr) {
                        break;
                    }
                    link_ref = &mut link.link;
                }
            }
        }
        cell.linked_entries -= 1;
    }

    pub fn consume_link(&mut self) -> Option<(K, V)> {
        let cell = self.get_cell_mut_ref();
        if cell.linked_entries == 0 {
            return None;
        }

        if let Some(result) = cell.link.as_mut().map(|head| head.consume_first_entry()) {
            if let Some(mut head) = result.0 {
                cell.link = head.take();
            }
            cell.linked_entries -= 1;
            return result.1;
        }
        None
    }

    pub fn num_linked_entries(&self) -> usize {
        self.cell.linked_entries
    }

    pub fn empty(&self) -> bool {
        (self.metadata & OCCUPANCY_MASK) == 0 && self.cell.linked_entries == 0
    }

    pub fn kill(&mut self) {
        debug_assert!(self.empty());
        self.metadata = self.metadata | KILLED_FLAG;
    }

    pub fn killed(&self) -> bool {
        self.metadata & KILLED_FLAG == KILLED_FLAG
    }

    fn get_cell_mut_ref(&mut self) -> &mut Cell<K, V> {
        let cell_ptr = self.cell as *const Cell<K, V>;
        let cell_mut_ptr = cell_ptr as *mut Cell<K, V>;
        unsafe { &mut (*cell_mut_ptr) }
    }
}

/// CellReader
pub struct CellReader<'a, K: Clone + Eq, V> {
    cell: &'a Cell<K, V>,
    metadata: u32,
}

impl<'a, K: Clone + Eq, V> CellReader<'a, K, V> {
    /// Create a new CellReader instance with the cell shared locked.
    pub fn lock(cell: &'a Cell<K, V>) -> CellReader<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell)) {
                return result;
            }
        }
    }

    /// Create a new CellReader instance if the cell is shared locked.
    fn try_lock(cell: &'a Cell<K, V>) -> Option<CellReader<'a, K, V>> {
        let mut current = cell.metadata.load(Relaxed);
        loop {
            if current & LOCK_MASK >= SLOCK_MAX {
                current = current & (!LOCK_MASK);
            }
            debug_assert_eq!(current & LOCK_MASK & XLOCK, 0);
            debug_assert!(current & LOCK_MASK < SLOCK_MAX);
            match cell
                .metadata
                .compare_exchange(current, current + SLOCK, Acquire, Relaxed)
            {
                Ok(result) => {
                    return Some(CellReader {
                        cell: cell,
                        metadata: result,
                    })
                }
                Err(result) => {
                    if result & LOCK_MASK >= SLOCK_MAX {
                        return None;
                    }
                    current = result;
                }
            }
        }
    }

    pub fn get(&self, key: &K, partial_hash: u16) -> Option<(u8, *const (K, V))> {
        if self.cell.linked_entries > 0 {
            let mut link_ref = &self.cell.link;
            while let Some(link) = link_ref.as_ref() {
                if let Some(result) = link.search_entry(key, partial_hash) {
                    return Some((u8::MAX, result.1));
                }
                link_ref = &link.link;
            }
        }
        let preferred_index = (partial_hash % (ARRAY_SIZE as u16)).try_into().unwrap();
        if self.cell.partial_hash_array[preferred_index as usize] == partial_hash
            && (self.metadata & (OCCUPANCY_BIT << preferred_index)) != 0
        {
            return Some((preferred_index, ptr::null()));
        }
        for i in 0..ARRAY_SIZE {
            if i != preferred_index
                && self.cell.partial_hash_array[i as usize] == partial_hash
                && (self.metadata & (OCCUPANCY_BIT << i)) != 0
            {
                return Some((i, ptr::null()));
            }
        }
        None
    }
}

struct WaitQueueEntry {
    mutex: Mutex<bool>,
    condvar: Condvar,
    next: *mut WaitQueueEntry,
}

impl WaitQueueEntry {
    fn new(wait_queue: *mut WaitQueueEntry) -> WaitQueueEntry {
        WaitQueueEntry {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
            next: wait_queue,
        }
    }

    fn wait(&self) {
        let mut completed = self.mutex.lock().unwrap();
        while !*completed {
            completed = self.condvar.wait(completed).unwrap();
        }
    }

    fn signal(&self) {
        let mut completed = self.mutex.lock().unwrap();
        *completed = true;
        self.condvar.notify_one();
    }
}

impl<'a, K: Clone + Eq, V> Drop for CellLocker<'a, K, V> {
    fn drop(&mut self) {
        // a Release fence is required to publish the changes
        let mut current = self.cell.metadata.load(Relaxed);
        loop {
            debug_assert_eq!(current & LOCK_MASK, XLOCK);
            match self.cell.metadata.compare_exchange(
                current,
                self.metadata & (!(WAITING_FLAG | XLOCK)),
                Release,
                Relaxed,
            ) {
                Ok(result) => {
                    if result & WAITING_FLAG == WAITING_FLAG {
                        self.cell.wakeup();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

impl<'a, K: Clone + Eq, V> Drop for CellReader<'a, K, V> {
    fn drop(&mut self) {
        // no modification is allowed with a CellReader held: no memory fences required
        let mut current = self.cell.metadata.load(Relaxed);
        loop {
            debug_assert!(current & LOCK_MASK <= SLOCK_MAX);
            debug_assert!(current & LOCK_MASK >= SLOCK);
            match self.cell.metadata.compare_exchange(
                current,
                (current & (!WAITING_FLAG)) - SLOCK,
                Relaxed,
                Relaxed,
            ) {
                Ok(result) => {
                    if result & WAITING_FLAG == WAITING_FLAG {
                        self.cell.wakeup();
                    }
                    break;
                }
                Err(result) => current = result,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn static_assertions() {
        assert_eq!(std::mem::size_of::<Cell<u64, bool>>(), 64);
        assert!(XLOCK > SLOCK_MAX);
        assert_eq!(WAITING_FLAG & LOCK_MASK, 0);
        assert!((XLOCK & LOCK_MASK) > SLOCK_MAX);
        assert_eq!(((XLOCK << 1) & LOCK_MASK), 0);
        assert_eq!(XLOCK & LOCK_MASK, XLOCK);
        assert_eq!(SLOCK & (!LOCK_MASK), 0);
        assert_eq!(SLOCK & LOCK_MASK, SLOCK);
        assert_eq!((SLOCK >> 1) & LOCK_MASK, 0);
        assert_eq!(SLOCK & SLOCK_MAX, SLOCK);
        assert_eq!(KILLED_FLAG & LOCK_MASK, 0);
        assert_eq!(LOCK_MASK & OCCUPANCY_MASK, 0);
        assert_eq!(
            KILLED_FLAG | WAITING_FLAG | LOCK_MASK | OCCUPANCY_MASK,
            !(0 as u32)
        );
    }

    #[test]
    fn basic_locker() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize>> = Arc::new(Default::default());
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(num_threads);
        for tid in 0..num_threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                for i in 0..4096 {
                    if i % 2 == 0 {
                        let mut xlocker = CellLocker::lock(&*cell_copied);
                        let mut sum: u64 = 0;
                        for j in 0..128 {
                            unsafe {
                                sum += (*data_ptr.load(Relaxed))[j];
                                (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                            };
                        }
                        assert_eq!(sum % 256, 0);
                        if i == 1024 {
                            xlocker.insert(tid.try_into().unwrap());
                        } else if i == 512 {
                            let key = tid + num_threads;
                            let inserted = xlocker.insert_link(&key, 1, i);
                            assert_eq!(unsafe { *(inserted.1) }, (key, 512));
                            assert!(xlocker.overflowing());
                            assert!(!xlocker.search_link(&key, 1).0.is_null());
                        }
                        drop(xlocker);
                    } else {
                        let slocker = CellReader::lock(&*cell_copied);
                        let mut sum: u64 = 0;
                        for j in 0..128 {
                            unsafe { sum += (*data_ptr.load(Relaxed))[j] };
                        }
                        assert_eq!(sum % 256, 0);
                        drop(slocker);
                    }
                }
            }));
        }
        for handle in thread_handles {
            handle.join().unwrap();
        }
        assert_eq!((*cell).size().0, ARRAY_SIZE as usize);
        assert_eq!((*cell).linked_entries, num_threads);
        for tid in 0..num_threads {
            let mut xlocker = CellLocker::lock(&*cell);
            let key = tid + num_threads;
            let result = xlocker.search_link(&key, 1);
            assert_ne!(result.0, ptr::null());
            assert_eq!(unsafe { *result.1 }, (key, 512));
            xlocker.remove_link(result.0, result.1);
            drop(xlocker);
            let xlocker_again = CellLocker::lock(&*cell);
            drop(xlocker_again);
        }
        assert_eq!((*cell).linked_entries, 0);
        assert_eq!((*cell).size().0, ARRAY_SIZE as usize);
        assert_eq!(
            (*cell).metadata.load(Relaxed) & OCCUPANCY_MASK,
            OCCUPANCY_MASK
        );
        for tid in 0..ARRAY_SIZE {
            let mut xlocker = CellLocker::lock(&*cell);
            xlocker.remove(tid.try_into().unwrap());
        }
        assert_eq!((*cell).size().0, 0);
        assert_eq!((*cell).metadata.load(Relaxed) & OCCUPANCY_MASK, 0);
        assert_eq!((*cell).metadata.load(Relaxed) & LOCK_MASK, 0);
    }
}
