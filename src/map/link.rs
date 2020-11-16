use std::mem::MaybeUninit;
use std::ptr;

pub const ARRAY_SIZE: usize = 4;

pub type LinkType<K, V> = Option<Box<EntryArrayLink<K, V>>>;

pub struct EntryArrayLink<K: Clone + Eq, V> {
    /// The array of partial hash values
    ///
    /// Zero represents a state where the corresponding entry is vacant.
    partial_hash_array: [u16; ARRAY_SIZE],
    entry_array: [MaybeUninit<(K, V)>; ARRAY_SIZE],
    link: LinkType<K, V>,
}

impl<K: Clone + Eq, V> EntryArrayLink<K, V> {
    pub fn new(link: LinkType<K, V>) -> EntryArrayLink<K, V> {
        EntryArrayLink {
            partial_hash_array: [0; ARRAY_SIZE],
            entry_array: unsafe { MaybeUninit::uninit().assume_init() },
            link: link,
        }
    }

    pub fn consume_first_entry(&mut self) -> (Option<LinkType<K, V>>, Option<(K, V)>) {
        let mut key_value = None;
        let mut empty = true;
        for i in 0..ARRAY_SIZE {
            if self.partial_hash_array[i] != 0 {
                if key_value.is_none() {
                    self.partial_hash_array[i] = 0;
                    let key_value_pair_entry_mut_ptr =
                        &mut self.entry_array[i] as *mut MaybeUninit<(K, V)>;
                    let entry = unsafe {
                        std::ptr::replace(key_value_pair_entry_mut_ptr, MaybeUninit::uninit())
                    };
                    let (key, value) = unsafe { entry.assume_init() };
                    key_value.replace((key, value));
                    if !empty {
                        break;
                    }
                } else {
                    empty = false;
                }
            }
        }
        if empty {
            (Some(self.link.take()), key_value)
        } else {
            (None, key_value)
        }
    }

    pub fn first_entry(&self) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        for i in 0..ARRAY_SIZE {
            if self.partial_hash_array[i] != 0 {
                return (
                    self as *const EntryArrayLink<K, V>,
                    self.entry_array[i].as_ptr(),
                );
            }
        }
        self.link
            .as_ref()
            .map_or((ptr::null(), ptr::null()), |link| (*link).first_entry())
    }

    pub fn next_entry(
        &self,
        entry_ptr: *const (K, V),
    ) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        for i in 0..ARRAY_SIZE {
            if entry_ptr == self.entry_array[i].as_ptr() {
                for j in (i + 1)..ARRAY_SIZE {
                    if self.partial_hash_array[j] != 0 {
                        return (
                            self as *const EntryArrayLink<K, V>,
                            self.entry_array[j].as_ptr(),
                        );
                    }
                }
                break;
            }
        }
        self.link
            .as_ref()
            .map_or((ptr::null(), ptr::null()), |link| (*link).first_entry())
    }

    pub fn search_entry(
        &self,
        key: &K,
        partial_hash: u16,
    ) -> (*const EntryArrayLink<K, V>, *const (K, V)) {
        for (i, v) in self.partial_hash_array.iter().enumerate() {
            if *v == (partial_hash | 1) {
                if unsafe { &(*self.entry_array[i].as_ptr()).0 } == key {
                    return (
                        self as *const EntryArrayLink<K, V>,
                        self.entry_array[i].as_ptr(),
                    );
                }
            }
        }
        self.link
            .as_ref()
            .map_or((ptr::null(), ptr::null()), |link| {
                (*link).search_entry(key, partial_hash)
            })
    }

    pub fn insert_entry(
        &mut self,
        key: &K,
        partial_hash: u16,
        value: V,
    ) -> Result<(*const EntryArrayLink<K, V>, *const (K, V)), V> {
        for i in 0..ARRAY_SIZE {
            if self.partial_hash_array[i] == 0 {
                self.partial_hash_array[i] = partial_hash | 1;
                unsafe { self.entry_array[i].as_mut_ptr().write((key.clone(), value)) };
                return Ok((
                    self as *const EntryArrayLink<K, V>,
                    self.entry_array[i].as_ptr(),
                ));
            }
        }

        if let Some(link) = &mut self.link {
            return link.insert_entry(key, partial_hash, value);
        } else {
            return Err(value);
        }
    }

    pub fn remove_entry(&mut self, key_value_pair_ptr: *const (K, V)) -> bool {
        let mut removed = false;
        let mut vacant = true;
        for i in 0..ARRAY_SIZE {
            if !removed && self.entry_array[i].as_ptr() == key_value_pair_ptr {
                unsafe { std::ptr::drop_in_place(self.entry_array[i].as_mut_ptr()) };
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

    pub fn remove_next(&mut self, entry_array_link_ptr: *const EntryArrayLink<K, V>) {
        let next = self.link.as_mut().map_or(None, |next| {
            if next.compare_ptr(entry_array_link_ptr) {
                return Some(next.link.take());
            }
            None
        });
        if let Some(next) = next {
            self.link.take();
            self.link = next;
            return;
        }
        self.link
            .as_mut()
            .map(|next| next.remove_next(entry_array_link_ptr));
    }

    fn compare_ptr(&self, entry_array_link_ptr: *const EntryArrayLink<K, V>) -> bool {
        self as *const EntryArrayLink<K, V> == entry_array_link_ptr
    }
}
