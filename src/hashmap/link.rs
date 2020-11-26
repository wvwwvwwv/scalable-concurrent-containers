use std::mem::MaybeUninit;

pub const ARRAY_SIZE: usize = 4;

pub type LinkType<K, V> = Option<Box<EntryArrayLink<K, V>>>;

pub struct EntryArrayLink<K: Eq, V> {
    /// The array of partial hash values
    ///
    /// Zero represents a state where the corresponding entry is vacant.
    partial_hash_array: [u16; ARRAY_SIZE],
    entry_array: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: LinkType<K, V>,
}

impl<K: Eq, V> EntryArrayLink<K, V> {
    pub fn new(link: LinkType<K, V>) -> EntryArrayLink<K, V> {
        EntryArrayLink {
            partial_hash_array: [0; ARRAY_SIZE],
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            link,
        }
    }

    pub fn link_ref(&self) -> &LinkType<K, V> {
        &self.link
    }

    pub fn link_mut_ref(&mut self) -> &mut LinkType<K, V> {
        &mut self.link
    }

    pub fn first_entry(&self) -> Option<(*const EntryArrayLink<K, V>, *const (K, V))> {
        for i in 0..ARRAY_SIZE {
            if self.partial_hash_array[i] != 0 {
                return Some((
                    self as *const EntryArrayLink<K, V>,
                    self.entry_array[i].as_ptr(),
                ));
            }
        }

        // the call depth is guaranteed to be less than two
        self.link
            .as_ref()
            .map_or_else(|| None, |link| (*link).first_entry())
    }

    pub fn next_entry(
        &self,
        entry_ptr: *const (K, V),
    ) -> Option<(*const EntryArrayLink<K, V>, *const (K, V))> {
        for i in 0..ARRAY_SIZE {
            if entry_ptr == self.entry_array[i].as_ptr() {
                for j in (i + 1)..ARRAY_SIZE {
                    if self.partial_hash_array[j] != 0 {
                        return Some((
                            self as *const EntryArrayLink<K, V>,
                            self.entry_array[j].as_ptr(),
                        ));
                    }
                }
                break;
            }
        }

        // the call depth is guaranteed to be less than two
        self.link
            .as_ref()
            .map_or_else(|| None, |link| (*link).first_entry())
    }

    pub fn search_entry(
        &self,
        key: &K,
        partial_hash: u16,
    ) -> Option<(*const EntryArrayLink<K, V>, *const (K, V))> {
        for (i, v) in self.partial_hash_array.iter().enumerate() {
            if *v != (partial_hash | 1) {
                continue;
            }
            if unsafe { &(*self.entry_array[i].as_ptr()).0 } == key {
                return Some((
                    self as *const EntryArrayLink<K, V>,
                    self.entry_array[i].as_ptr(),
                ));
            }
        }
        None
    }

    pub fn insert_entry(
        &mut self,
        key: K,
        partial_hash: u16,
        value: V,
    ) -> Result<(*const EntryArrayLink<K, V>, *const (K, V)), (K, V)> {
        for i in 0..ARRAY_SIZE {
            if self.partial_hash_array[i] == 0 {
                self.partial_hash_array[i] = partial_hash | 1;
                unsafe { self.entry_array[i].as_mut_ptr().write((key, value)) };
                return Ok((
                    self as *const EntryArrayLink<K, V>,
                    self.entry_array[i].as_ptr(),
                ));
            }
        }
        Err((key, value))
    }

    pub fn remove_entry(&mut self, drop_entry: bool, key_value_pair_ptr: *const (K, V)) -> bool {
        let mut removed = false;
        let mut vacant = true;
        for i in 0..ARRAY_SIZE {
            if !removed && self.entry_array[i].as_ptr() == key_value_pair_ptr {
                if drop_entry {
                    unsafe { std::ptr::drop_in_place(self.entry_array[i].as_mut_ptr()) };
                }
                self.partial_hash_array[i] = 0;
                if !vacant {
                    return false;
                }
                removed = true;
            } else if vacant && self.partial_hash_array[i] != 0 {
                if removed {
                    return false;
                }
                vacant = false;
            }
        }
        vacant
    }

    pub fn remove_self(
        &mut self,
        entry_array_link_ptr: *const EntryArrayLink<K, V>,
    ) -> Result<LinkType<K, V>, ()> {
        if self.compare_ptr(entry_array_link_ptr) {
            return Ok(self.link.take());
        }
        Err(())
    }

    pub fn remove_next(&mut self, entry_array_link_ptr: *const EntryArrayLink<K, V>) -> bool {
        let next = self.link.as_mut().map_or_else(
            || None,
            |next| {
                if next.compare_ptr(entry_array_link_ptr) {
                    return Some(next.link.take());
                }
                None
            },
        );
        if let Some(next) = next {
            self.link.take();
            self.link = next;
            return true;
        }
        false
    }

    fn compare_ptr(&self, entry_array_link_ptr: *const EntryArrayLink<K, V>) -> bool {
        self as *const EntryArrayLink<K, V> == entry_array_link_ptr
    }
}
