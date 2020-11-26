use super::link::{EntryArrayLink, LinkType};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: u8 = 16;
const KILLED_FLAG: u32 = 1u32 << 31;
const WAITING_FLAG: u32 = 1u32 << 30;
const LOCK_MASK: u32 = ((1u32 << 14) - 1) << ARRAY_SIZE;
const XLOCK: u32 = 1u32 << 29;
const SLOCK_MAX: u32 = LOCK_MASK & (!XLOCK);
const SLOCK: u32 = 1u32 << ARRAY_SIZE;
const OCCUPANCY_MASK: u32 = (1u32 << ARRAY_SIZE) - 1;
const OCCUPANCY_BIT: u32 = 1;

pub type EntryArray<K, V> = [MaybeUninit<(K, V)>; ARRAY_SIZE as usize];

pub struct Cell<K: Eq, V> {
    partial_hash_array: [u16; ARRAY_SIZE as usize],
    metadata: AtomicU32,
    wait_queue: AtomicPtr<WaitQueueEntry>,
    link: LinkType<K, V>,
    linked_entries: usize,
}

impl<K: Eq, V> Default for Cell<K, V> {
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

impl<K: Eq, V> Cell<K, V> {
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

    fn search(
        &self,
        metadata: u32,
        key: &K,
        partial_hash: u16,
        entry_array: &EntryArray<K, V>,
    ) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        let occupancy_metadata = metadata & OCCUPANCY_MASK;

        // start with the preferred index
        let preferred_index = (partial_hash % (ARRAY_SIZE as u16)).try_into().unwrap();
        if (occupancy_metadata & (OCCUPANCY_BIT << preferred_index)) != 0
            && self.partial_hash_array[preferred_index as usize] == partial_hash
        {
            let entry_ptr = entry_array[preferred_index as usize].as_ptr();
            if unsafe { &(*entry_ptr) }.0 == *key {
                return Some((preferred_index, ptr::null(), entry_ptr));
            }
        }

        // iterate the array
        let start_index: u8 = occupancy_metadata.trailing_zeros().try_into().unwrap();
        for i in start_index..ARRAY_SIZE {
            let occupancy_bit = OCCUPANCY_BIT << i;
            if occupancy_bit > occupancy_metadata {
                break;
            }
            if i != preferred_index
                && (occupancy_metadata & occupancy_bit) != 0
                && self.partial_hash_array[i as usize] == partial_hash
            {
                let entry_ptr = entry_array[i as usize].as_ptr();
                if unsafe { &(*entry_ptr) }.0 == *key {
                    return Some((i, ptr::null(), entry_ptr));
                }
            }
        }

        // traverse the link
        let mut link_ref = &self.link;
        while let Some(link) = link_ref.as_ref() {
            if let Some(result) = link.search_entry(key, partial_hash) {
                return Some((u8::MAX, result.0, result.1));
            }
            link_ref = &link.link_ref();
        }

        None
    }
}

/// CellLocker
pub struct CellLocker<'a, K: Eq, V> {
    cell: &'a Cell<K, V>,
    entry_array: &'a EntryArray<K, V>,
    metadata: u32,
}

impl<'a, K: Eq, V> CellLocker<'a, K, V> {
    /// Create a new CellLocker instance with the cell exclusively locked.
    pub fn lock(cell: &'a Cell<K, V>, entry_array: &'a EntryArray<K, V>) -> CellLocker<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell, entry_array) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell, entry_array)) {
                return result;
            }
        }
    }

    /// Create a new CellLocker instance if the cell is exclusively locked.
    fn try_lock(
        cell: &'a Cell<K, V>,
        entry_array: &'a EntryArray<K, V>,
    ) -> Option<CellLocker<'a, K, V>> {
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
                        entry_array: entry_array,
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

    pub fn occupied(&self, sub_index: u8) -> bool {
        (self.metadata & (OCCUPANCY_BIT << sub_index)) != 0
    }

    pub fn full(&self) -> bool {
        (self.metadata & OCCUPANCY_MASK) == OCCUPANCY_MASK
    }

    pub fn partial_hash(&self, sub_index: u8) -> u16 {
        self.cell.partial_hash_array[sub_index as usize]
    }

    pub fn next(
        &mut self,
        erase_current: bool,
        drop_entry: bool,
        sub_index: u8,
        entry_array_link_ptr: *const EntryArrayLink<K, V>,
        entry_ptr: *const (K, V),
    ) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        if !entry_array_link_ptr.is_null() {
            // traverse the link
            let next = unsafe { (*entry_array_link_ptr).next_entry(entry_ptr) };
            // erase the linked entry
            if erase_current {
                self.remove(drop_entry, u8::MAX, entry_array_link_ptr, entry_ptr);
            }
            if let Some(next) = next {
                return Some((u8::MAX, next.0, next.1));
            }
        }

        // erase the entry
        if erase_current && sub_index != u8::MAX {
            self.remove(drop_entry, sub_index, std::ptr::null(), std::ptr::null());
        }

        // advance in the cell
        let occupancy_metadata = self.metadata & OCCUPANCY_MASK;
        let start_index = if sub_index == u8::MAX {
            occupancy_metadata.trailing_zeros().try_into().unwrap()
        } else {
            sub_index + 1
        };
        for i in start_index..ARRAY_SIZE {
            let occupancy_bit = OCCUPANCY_BIT << i;
            if occupancy_bit > occupancy_metadata {
                break;
            }
            if (occupancy_metadata & occupancy_bit) != 0 {
                let entry_ptr = self.entry_array[i as usize].as_ptr();
                return Some((i, ptr::null(), entry_ptr));
            }
        }
        None
    }

    pub fn search(
        &self,
        key: &K,
        partial_hash: u16,
    ) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        self.cell
            .search(self.metadata, key, partial_hash, self.entry_array)
    }

    pub fn insert(
        &mut self,
        key: K,
        partial_hash: u16,
        value: V,
    ) -> (u8, *const EntryArrayLink<K, V>, *const (K, V)) {
        for i in [
            (partial_hash % (ARRAY_SIZE as u16)) as usize,
            self.metadata.trailing_ones() as usize,
        ]
        .iter()
        {
            if *i >= ARRAY_SIZE as usize {
                continue;
            }
            if !self.occupied((*i).try_into().unwrap()) {
                unsafe {
                    self.entry_array_mut_ref()[*i]
                        .as_mut_ptr()
                        .write((key, value))
                };
                self.metadata = self.metadata | (OCCUPANCY_BIT << i);
                self.cell_mut_ref().partial_hash_array[*i] = partial_hash;
                return (
                    (*i).try_into().unwrap(),
                    ptr::null(),
                    self.entry_array[*i].as_ptr(),
                );
            }
        }

        let cell = self.cell_mut_ref();
        let mut key = key;
        let mut value = value;
        let mut link_ref = &mut cell.link;
        while let Some(link) = link_ref.as_mut() {
            match link.insert_entry(key, partial_hash, value) {
                Ok(result) => {
                    cell.linked_entries += 1;
                    return (u8::MAX, result.0, result.1);
                }
                Err(result) => {
                    key = result.0;
                    value = result.1;
                }
            }
            link_ref = link.link_mut_ref();
        }

        let mut new_entry_array_link = Box::new(EntryArrayLink::new(cell.link.take()));
        let result = new_entry_array_link.insert_entry(key, partial_hash, value);
        cell.link = Some(new_entry_array_link);
        cell.linked_entries += 1;
        let result = result.ok().unwrap();
        (u8::MAX, result.0, result.1)
    }

    pub fn remove(
        &mut self,
        drop_entry: bool,
        sub_index: u8,
        entry_array_link_ptr: *const EntryArrayLink<K, V>,
        key_value_pair_ptr: *const (K, V),
    ) {
        if sub_index != u8::MAX {
            self.metadata = self.metadata & (!(OCCUPANCY_BIT << sub_index));
            if drop_entry {
                unsafe {
                    std::ptr::drop_in_place(
                        self.entry_array_mut_ref()[sub_index as usize].as_mut_ptr(),
                    );
                };
            }
        }
        if !entry_array_link_ptr.is_null() {
            let entry_array_link_mut_ptr = entry_array_link_ptr as *mut EntryArrayLink<K, V>;
            let cell = self.cell_mut_ref();
            if unsafe { (*entry_array_link_mut_ptr).remove_entry(drop_entry, key_value_pair_ptr) } {
                if let Ok(mut head) = cell.link.as_mut().map_or_else(
                    || Err(()),
                    |head| head.remove_self(entry_array_link_mut_ptr),
                ) {
                    cell.link = head.take();
                } else {
                    let mut link_ref = &mut cell.link;
                    while let Some(link) = link_ref.as_mut() {
                        if link.remove_next(entry_array_link_mut_ptr) {
                            break;
                        }
                        link_ref = link.link_mut_ref();
                    }
                }
            }
            cell.linked_entries -= 1;
        }
    }

    pub fn first(&self) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        self.cell.link.as_ref().map_or_else(
            || {
                let start_index: u8 = self.metadata.trailing_zeros().try_into().unwrap();
                for i in start_index..ARRAY_SIZE {
                    if self.occupied(i) {
                        return Some((i, ptr::null(), self.entry_array[i as usize].as_ptr()));
                    }
                }
                None
            },
            |entry| {
                if let Some(result) = entry.first_entry() {
                    return Some((u8::MAX, result.0, result.1));
                }
                None
            },
        )
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

    fn cell_mut_ref(&mut self) -> &mut Cell<K, V> {
        let cell_ptr = self.cell as *const Cell<K, V>;
        let cell_mut_ptr = cell_ptr as *mut Cell<K, V>;
        unsafe { &mut (*cell_mut_ptr) }
    }

    fn entry_array_mut_ref(&mut self) -> &mut EntryArray<K, V> {
        let entry_array_ptr = self.entry_array as *const EntryArray<K, V>;
        let entry_array_mut_ptr = entry_array_ptr as *mut EntryArray<K, V>;
        unsafe { &mut (*entry_array_mut_ptr) }
    }
}

impl<'a, K: Eq, V> Drop for CellLocker<'a, K, V> {
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

/// CellReader
pub struct CellReader<'a, K: Eq, V> {
    cell: &'a Cell<K, V>,
    entry_array: &'a EntryArray<K, V>,
    metadata: u32,
}

impl<'a, K: Eq, V> CellReader<'a, K, V> {
    /// Create a new CellReader instance with the cell shared locked.
    pub fn lock(cell: &'a Cell<K, V>, entry_array: &'a EntryArray<K, V>) -> CellReader<'a, K, V> {
        loop {
            if let Some(result) = Self::try_lock(cell, entry_array) {
                return result;
            }
            if let Some(result) = cell.wait(|| Self::try_lock(cell, entry_array)) {
                return result;
            }
        }
    }

    /// Create a new CellReader instance if the cell is shared locked.
    fn try_lock(
        cell: &'a Cell<K, V>,
        entry_array: &'a EntryArray<K, V>,
    ) -> Option<CellReader<'a, K, V>> {
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
                        entry_array: entry_array,
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

    pub fn search(&self, key: &K, partial_hash: u16) -> Option<*const (K, V)> {
        self.cell
            .search(self.metadata, key, partial_hash, self.entry_array)
            .as_ref()
            .map(|result| result.2)
    }
}

impl<'a, K: Eq, V> Drop for CellReader<'a, K, V> {
    fn drop(&mut self) {
        // no modification is allowed with a CellReader held: no memory fences required
        let mut current = self.metadata + SLOCK;
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
    fn cell_locker() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize>> = Arc::new(Default::default());
        let entry_array: Arc<EntryArray<usize, usize>> =
            Arc::new(unsafe { MaybeUninit::uninit().assume_init() });
        let mut data: [u64; 128] = [0; 128];
        let mut thread_handles = Vec::with_capacity(num_threads);
        for tid in 0..num_threads {
            let barrier_copied = barrier.clone();
            let cell_copied = cell.clone();
            let entry_array_copied = entry_array.clone();
            let data_ptr = AtomicPtr::new(&mut data);
            thread_handles.push(thread::spawn(move || {
                barrier_copied.wait();
                for i in 0..4096 {
                    if i % 2 == 0 {
                        let mut xlocker = CellLocker::lock(&*cell_copied, &*entry_array_copied);
                        let mut sum: u64 = 0;
                        for j in 0..128 {
                            unsafe {
                                sum += (*data_ptr.load(Relaxed))[j];
                                (*data_ptr.load(Relaxed))[j] = if i % 4 == 0 { 2 } else { 4 }
                            };
                        }
                        assert_eq!(sum % 256, 0);
                        if i == 1024 {
                            xlocker.insert(tid, tid.try_into().unwrap(), tid);
                        }
                        drop(xlocker);
                    } else {
                        let slocker = CellReader::lock(&*cell_copied, &*entry_array_copied);
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
        assert_eq!((*cell).size().0 + (*cell).size().1, num_threads);
        for tid in 0..(num_threads / 2) {
            let mut xlocker = CellLocker::lock(&*cell, &*entry_array);
            let result = xlocker.first();
            assert!(result.is_some());
            let result = xlocker.search(&tid, tid.try_into().unwrap());
            assert!(result.is_some());
            if let Some((sub_index, entry_array_link_ptr, entry_ptr)) = result {
                assert_eq!(unsafe { *entry_ptr }, (tid.try_into().unwrap(), tid));
                xlocker.remove(true, sub_index, entry_array_link_ptr, entry_ptr);
            }
        }
        let mut xlocker = CellLocker::lock(&*cell, &*entry_array);
        let mut current = xlocker.first();
        while let Some((sub_index, entry_array_link_ptr, entry_ptr)) = current {
            current = xlocker.next(true, true, sub_index, entry_array_link_ptr, entry_ptr);
        }
        drop(xlocker);
        assert_eq!((*cell).size().0, 0);
        assert_eq!((*cell).size().1, 0);
        assert_eq!((*cell).metadata.load(Relaxed) & OCCUPANCY_MASK, 0);
        assert_eq!((*cell).metadata.load(Relaxed) & LOCK_MASK, 0);
    }
}
