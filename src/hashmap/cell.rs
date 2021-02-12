use super::link::{EntryArrayLink, LinkType};
use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicU32};
use std::sync::{Condvar, Mutex};

pub const ARRAY_SIZE: usize = 16;
pub const MAX_RESIZING_FACTOR: usize = 6;
const THRESHOLD: u32 = 1u32 << 26;

const KILLED_FLAG: u32 = 1u32 << 31;
const WAITING_FLAG: u32 = 1u32 << 30;
const XLOCK: u32 = 1u32 << 29;
const SLOCK_MAX: u32 = XLOCK - 1;
const SLOCK: u32 = 1u32;
const LOCK_MASK: u32 = XLOCK | SLOCK_MAX;

pub struct EntryArray<K: Eq, V> {
    /// Zero implies that the corresponding position is vacant.
    partial_hash_array: [u8; ARRAY_SIZE],
    entries: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: LinkType<K, V>,
}

/// Cell is a 24-byte data structure that manages the metadata of key-value pairs.
pub struct Cell<K: Eq, V> {
    metadata: AtomicU32,
    num_entries: u32,
    wait_queue: AtomicPtr<WaitQueueEntry>,
    entry_array: Option<Box<EntryArray<K, V>>>,
}

impl<K: Eq, V> Default for Cell<K, V> {
    fn default() -> Self {
        Cell {
            metadata: AtomicU32::new(0),
            num_entries: 0,
            wait_queue: AtomicPtr::new(ptr::null_mut()),
            entry_array: None,
        }
    }
}

impl<K: Eq, V> Cell<K, V> {
    pub fn killed(&self) -> bool {
        self.metadata.load(Relaxed) & KILLED_FLAG == KILLED_FLAG
    }

    pub fn size(&self) -> usize {
        self.num_entries as usize
    }

    fn wait<T, F: FnOnce() -> Option<T>>(&self, f: F) -> Option<T> {
        // Inserts the condvar into the wait queue.
        let mut condvar = WaitQueueEntry::new(self.wait_queue.load(Relaxed));
        let condvar_ptr: *mut WaitQueueEntry = &mut condvar;

        // Inserts itself into the wait queue
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

        // Tries to lock again once the condvar is inserted into the wait queue.
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
            if condvar_ptr.is_null() {
                return;
            }
        }

        while !condvar_ptr.is_null() {
            let cond_var_ref = unsafe { &(*condvar_ptr) };
            let next_ptr = cond_var_ref.next;
            cond_var_ref.signal();
            condvar_ptr = next_ptr;
        }
    }

    fn search(
        &self,
        key: &K,
        partial_hash: u8,
    ) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        if let Some(entry_array) = self.entry_array.as_ref() {
            // Starts with the preferred index.
            let preferred_index = partial_hash % (ARRAY_SIZE as u8);
            if entry_array.partial_hash_array[preferred_index as usize] == (partial_hash | 1) {
                let entry_ptr = entry_array.entries[preferred_index as usize].as_ptr();
                if unsafe { &(*entry_ptr) }.0 == *key {
                    return Some((preferred_index, ptr::null(), entry_ptr));
                }
            }

            // Iterates the array.
            for i in 0..ARRAY_SIZE.try_into().unwrap() {
                if i != preferred_index
                    && entry_array.partial_hash_array[i as usize] == (partial_hash | 1)
                {
                    let entry_ptr = entry_array.entries[i as usize].as_ptr();
                    if unsafe { &(*entry_ptr) }.0 == *key {
                        return Some((i, ptr::null(), entry_ptr));
                    }
                }
            }

            // Traverses the link.
            let mut link_ref = &entry_array.link;
            while let Some(link) = link_ref.as_ref() {
                if let Some(result) = link.search_entry(key, partial_hash) {
                    return Some((u8::MAX, result.0, result.1));
                }
                link_ref = &link.link_ref();
            }
        }
        None
    }
}

impl<K: Eq, V> Drop for Cell<K, V> {
    fn drop(&mut self) {
        // If it has been killed, nothing to cleanup.
        if let Some(mut entry_array) = self.entry_array.take() {
            let metadata = self.metadata.load(Acquire);
            if metadata & KILLED_FLAG == 0 {
                // Iterates the array.
                for i in 0..ARRAY_SIZE {
                    if entry_array.partial_hash_array[i as usize] != 0 {
                        unsafe {
                            std::ptr::drop_in_place(entry_array.entries[i].as_mut_ptr());
                        }
                    }
                }

                // Traverses the link.
                let mut link_option = entry_array.link.take();
                while let Some(link) = link_option {
                    link_option = link.cleanup();
                }
            }
        }
    }
}

/// CellLocker.
pub struct CellLocker<'a, K: Eq, V> {
    cell: &'a Cell<K, V>,
    metadata: u32,
}

impl<'a, K: Eq, V> CellLocker<'a, K, V> {
    /// Creates a new CellLocker instance with the cell exclusively locked.
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

    /// Creates a new CellLocker instance if the cell is exclusively locked.
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
                    return Some(CellLocker {
                        cell,
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

    pub fn size(&self) -> usize {
        self.cell.size()
    }

    pub fn partial_hash(&self, sub_index: u8) -> u8 {
        self.cell.entry_array.as_ref().map_or_else(
            || 0,
            |entry_array| entry_array.partial_hash_array[sub_index as usize] & (!1u8),
        )
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
            // Traverses the link.
            let next = unsafe { (*entry_array_link_ptr).next_entry(entry_ptr) };
            // Erases the linked entry.
            if erase_current {
                self.remove(drop_entry, u8::MAX, entry_array_link_ptr, entry_ptr);
            }
            if let Some(next) = next {
                return Some((u8::MAX, next.0, next.1));
            }
        }

        // Erases the entry.
        if erase_current && sub_index != u8::MAX {
            self.remove(drop_entry, sub_index, std::ptr::null(), std::ptr::null());
        }

        // Advances in the cell.
        if let Some(entry_array) = self.cell.entry_array.as_ref() {
            let start_index = if sub_index == u8::MAX {
                0
            } else {
                sub_index + 1
            };
            for i in start_index..ARRAY_SIZE.try_into().unwrap() {
                if entry_array.partial_hash_array[i as usize] != 0 {
                    let entry_ptr = entry_array.entries[i as usize].as_ptr();
                    return Some((i, ptr::null(), entry_ptr));
                }
            }
        }
        None
    }

    pub fn search(
        &self,
        key: &K,
        partial_hash: u8,
    ) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        self.cell.search(key, partial_hash)
    }

    pub fn insert(
        &mut self,
        key: K,
        partial_hash: u8,
        value: V,
        check_num_entries: bool,
    ) -> Result<(u8, *const EntryArrayLink<K, V>, *const (K, V)), V> {
        let cell_mut_ref = self.cell_mut_ref();
        if cell_mut_ref.entry_array.is_none() {
            // Allocates a new entry array.
            debug_assert_eq!(cell_mut_ref.num_entries, 0);
            cell_mut_ref.entry_array.replace(Box::new(EntryArray {
                partial_hash_array: [0; ARRAY_SIZE],
                entries: unsafe { MaybeUninit::uninit().assume_init() },
                link: None,
            }));
        }
        let entry_array_mut_ref = cell_mut_ref.entry_array.as_mut().unwrap();

        let preferred_index = (partial_hash % (ARRAY_SIZE as u8)) as usize;
        if entry_array_mut_ref.partial_hash_array[preferred_index] == 0 {
            unsafe {
                entry_array_mut_ref.entries[preferred_index]
                    .as_mut_ptr()
                    .write((key, value))
            };
            cell_mut_ref.num_entries += 1;
            entry_array_mut_ref.partial_hash_array[preferred_index] = partial_hash | 1;
            return Ok((
                preferred_index.try_into().unwrap(),
                ptr::null(),
                entry_array_mut_ref.entries[preferred_index].as_ptr(),
            ));
        }

        // Iterates the array.
        for i in 0..ARRAY_SIZE {
            if i != preferred_index && entry_array_mut_ref.partial_hash_array[i] == 0 {
                unsafe {
                    entry_array_mut_ref.entries[i]
                        .as_mut_ptr()
                        .write((key, value))
                };
                cell_mut_ref.num_entries += 1;
                entry_array_mut_ref.partial_hash_array[i] = partial_hash | 1;
                return Ok((
                    i.try_into().unwrap(),
                    ptr::null(),
                    entry_array_mut_ref.entries[i].as_ptr(),
                ));
            }
        }

        // If the cell contains more entries than the threshold, returns None.
        if check_num_entries && cell_mut_ref.num_entries >= THRESHOLD {
            return Err(value);
        }

        let mut key = key;
        let mut value = value;
        let mut link_ref = &mut entry_array_mut_ref.link;
        while let Some(link) = link_ref.as_mut() {
            match link.insert_entry(key, partial_hash, value) {
                Ok(result) => {
                    cell_mut_ref.num_entries += 1;
                    return Ok((u8::MAX, result.0, result.1));
                }
                Err(result) => {
                    key = result.0;
                    value = result.1;
                }
            }
            link_ref = link.link_mut_ref();
        }

        let mut new_entry_array_link =
            Box::new(EntryArrayLink::new(entry_array_mut_ref.link.take()));
        let result = new_entry_array_link.insert_entry(key, partial_hash, value);
        entry_array_mut_ref.link.replace(new_entry_array_link);
        let result = result.ok().unwrap();
        cell_mut_ref.num_entries += 1;

        Ok((u8::MAX, result.0, result.1))
    }

    pub fn remove(
        &mut self,
        drop_entry: bool,
        sub_index: u8,
        entry_array_link_ptr: *const EntryArrayLink<K, V>,
        key_value_pair_ptr: *const (K, V),
    ) {
        let cell_mut_ref = self.cell_mut_ref();
        debug_assert!(cell_mut_ref.entry_array.is_some());
        cell_mut_ref.num_entries -= 1;
        if sub_index != u8::MAX {
            let entry_array_mut_ref = cell_mut_ref.entry_array.as_mut().unwrap();
            entry_array_mut_ref.partial_hash_array[sub_index as usize] = 0;
            if drop_entry {
                unsafe {
                    std::ptr::drop_in_place(
                        entry_array_mut_ref.entries[sub_index as usize].as_mut_ptr(),
                    );
                };
            }
            if cell_mut_ref.num_entries == 0 {
                // Drops the entry array when all the entries are removed.
                cell_mut_ref.entry_array.take();
            }
        } else {
            let entry_array_mut_ref = cell_mut_ref.entry_array.as_mut().unwrap();
            if !entry_array_link_ptr.is_null() {
                let entry_array_link_mut_ptr = entry_array_link_ptr as *mut EntryArrayLink<K, V>;
                if unsafe {
                    (*entry_array_link_mut_ptr).remove_entry(drop_entry, key_value_pair_ptr)
                } {
                    if let Ok(mut head) = entry_array_mut_ref.link.as_mut().map_or_else(
                        || Err(()),
                        |head| head.remove_self(entry_array_link_mut_ptr),
                    ) {
                        entry_array_mut_ref.link = head.take();
                    } else {
                        let mut link_ref = &mut entry_array_mut_ref.link;
                        while let Some(link) = link_ref.as_mut() {
                            if link.remove_next(entry_array_link_mut_ptr) {
                                break;
                            }
                            link_ref = link.link_mut_ref();
                        }
                    }
                }
            }
        }
    }

    pub fn first(&self) -> Option<(u8, *const EntryArrayLink<K, V>, *const (K, V))> {
        if let Some(entry_array_ref) = self.cell.entry_array.as_ref() {
            entry_array_ref.link.as_ref().map_or_else(
                || {
                    for i in 0..ARRAY_SIZE.try_into().unwrap() {
                        if entry_array_ref.partial_hash_array[i as usize] != 0 {
                            return Some((
                                i,
                                ptr::null(),
                                entry_array_ref.entries[i as usize].as_ptr(),
                            ));
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
        } else {
            None
        }
    }

    pub fn empty(&self) -> bool {
        self.cell.num_entries == 0
    }

    pub fn kill(&mut self) {
        debug_assert!(self.empty());
        self.metadata |= KILLED_FLAG;
    }

    pub fn killed(&self) -> bool {
        self.metadata & KILLED_FLAG == KILLED_FLAG
    }

    fn cell_mut_ref(&mut self) -> &mut Cell<K, V> {
        let cell_ptr = self.cell as *const Cell<K, V>;
        let cell_mut_ptr = cell_ptr as *mut Cell<K, V>;
        unsafe { &mut (*cell_mut_ptr) }
    }
}

impl<'a, K: Eq, V> Drop for CellLocker<'a, K, V> {
    fn drop(&mut self) {
        // a Release fence is required to publish the changes
        let mut current = self.cell.metadata.load(Relaxed);
        loop {
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

/// CellReader.
pub struct CellReader<'a, K: Eq, V> {
    cell: &'a Cell<K, V>,
    metadata: u32,
    entry_ptr: *const (K, V),
}

impl<'a, K: Eq, V> CellReader<'a, K, V> {
    /// Creates a new CellReader instance with the cell shared locked.
    pub fn read(cell: &'a Cell<K, V>, key: &K, partial_hash: u8) -> CellReader<'a, K, V> {
        loop {
            // Early exit: not locked and empty.
            let current = cell.metadata.load(Acquire);
            if cell.num_entries == 0 {
                return CellReader {
                    cell,
                    metadata: 0,
                    entry_ptr: ptr::null(),
                };
            }

            if let Some(result) = Self::try_read(cell, current, key, partial_hash) {
                return result;
            }
            if let Some(result) =
                cell.wait(|| Self::try_read(cell, cell.metadata.load(Relaxed), key, partial_hash))
            {
                return result;
            }
        }
    }

    /// Creates a new CellReader instance if the cell is shared locked.
    fn try_read(
        cell: &'a Cell<K, V>,
        metadata: u32,
        key: &K,
        partial_hash: u8,
    ) -> Option<CellReader<'a, K, V>> {
        let mut current = metadata;
        loop {
            if current & LOCK_MASK >= SLOCK_MAX {
                // It is bound to fail.
                current &= !LOCK_MASK;
            }
            match cell
                .metadata
                .compare_exchange(current, current + SLOCK, Acquire, Relaxed)
            {
                Ok(result) => {
                    return Some(CellReader {
                        cell,
                        metadata: result + SLOCK,
                        entry_ptr: cell
                            .search(key, partial_hash)
                            .as_ref()
                            .map_or_else(ptr::null, |result| result.2),
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

    pub fn get(&self) -> Option<(&K, &V)> {
        if self.entry_ptr.is_null() {
            return None;
        }
        let entry_ref = unsafe { &(*self.entry_ptr) };
        Some((&entry_ref.0, &entry_ref.1))
    }
}

impl<'a, K: Eq, V> Drop for CellReader<'a, K, V> {
    fn drop(&mut self) {
        if self.metadata == 0 {
            return;
        }

        // No modification is allowed with a CellReader held: no memory fences required.
        let mut current = self.metadata;
        loop {
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
    fn cell_locker() {
        let num_threads = (ARRAY_SIZE + 1) as usize;
        let barrier = Arc::new(Barrier::new(num_threads));
        let cell: Arc<Cell<usize, usize>> = Arc::new(Default::default());
        let mut xlocker = CellLocker::lock(&*cell);
        let result = xlocker.insert(usize::MAX, 0, usize::MAX, true);
        assert!(result.is_ok());
        drop(xlocker);
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
                            let result = xlocker.insert(tid, tid.try_into().unwrap(), tid, true);
                            assert!(result.is_ok());
                        }
                        drop(xlocker);
                    } else {
                        let slocker = CellReader::read(&*cell_copied, &usize::MAX, 0);
                        if let Some((key, value)) = slocker.get() {
                            assert_eq!(*key, usize::MAX);
                            assert_eq!(*value, usize::MAX);
                        }
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
        assert_eq!((*cell).size(), num_threads + 1);
        let mut xlocker = CellLocker::lock(&*cell);
        let result = xlocker.search(&usize::MAX, 0);
        if let Some((sub_index, entry_array_link_ptr, entry_ptr)) = result {
            xlocker.remove(true, sub_index, entry_array_link_ptr, entry_ptr);
        }
        drop(xlocker);
        for tid in 0..(num_threads / 2) {
            let mut xlocker = CellLocker::lock(&*cell);
            let result = xlocker.first();
            assert!(result.is_some());
            let result = xlocker.search(&tid, tid.try_into().unwrap());
            assert!(result.is_some());
            if let Some((sub_index, entry_array_link_ptr, entry_ptr)) = result {
                assert_eq!(unsafe { *entry_ptr }, (tid.try_into().unwrap(), tid));
                xlocker.remove(true, sub_index, entry_array_link_ptr, entry_ptr);
            }
        }
        let mut xlocker = CellLocker::lock(&*cell);
        let mut current = xlocker.first();
        while let Some((sub_index, entry_array_link_ptr, entry_ptr)) = current {
            current = xlocker.next(true, true, sub_index, entry_array_link_ptr, entry_ptr);
        }
        drop(xlocker);
        assert_eq!((*cell).size(), 0);
        assert_eq!((*cell).metadata.load(Relaxed) & LOCK_MASK, 0);
    }
}
