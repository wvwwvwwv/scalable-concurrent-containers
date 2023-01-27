//! This module implements helper types and traits for `serde`.

use super::ebr::Barrier;
use super::{HashIndex, HashMap, HashSet, TreeIndex};

use serde::de::{Deserialize, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde::Deserializer;

use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

/// Helper type to allow `serde` to access [`HashMap`] entries.
pub struct HashMapVisitor<K: Eq + Hash + Sync, V: Sync, H: BuildHasher> {
    #[allow(clippy::type_complexity)]
    marker: PhantomData<fn() -> HashMap<K, V, H>>,
}

impl<K, V, H> HashMapVisitor<K, V, H>
where
    K: Eq + Hash + Sync,
    V: Sync,
    H: BuildHasher,
{
    fn new() -> Self {
        HashMapVisitor {
            marker: PhantomData,
        }
    }
}

impl<'d, K, V, H> Visitor<'d> for HashMapVisitor<K, V, H>
where
    K: Deserialize<'d> + Eq + Hash + Sync,
    V: Deserialize<'d> + Sync,
    H: BuildHasher + Default,
{
    type Value = HashMap<K, V, H>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("HashMap")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'d>,
    {
        let hashmap =
            HashMap::with_capacity_and_hasher(access.size_hint().unwrap_or(0), H::default());
        while let Some((key, val)) = access.next_entry()? {
            let _result = hashmap.insert(key, val);
        }
        Ok(hashmap)
    }
}

impl<'d, K, V, H> Deserialize<'d> for HashMap<K, V, H>
where
    K: Deserialize<'d> + Eq + Hash + Sync,
    V: Deserialize<'d> + Sync,
    H: BuildHasher + Default,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        deserializer.deserialize_map(HashMapVisitor::<K, V, H>::new())
    }
}

impl<K, V, H> Serialize for HashMap<K, V, H>
where
    K: Eq + Hash + Serialize + Sync,
    V: Serialize + Sync,
    H: BuildHasher,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        let mut error = None;
        self.for_each(|k, v| {
            if error.is_none() {
                if let Err(e) = map.serialize_entry(k, v) {
                    error.replace(e);
                }
            }
        });
        if let Some(e) = error {
            return Err(e);
        }
        map.end()
    }
}

/// Helper type to allow `serde` to access [`HashSet`] entries.
pub struct HashSetVisitor<K: Eq + Hash + Sync, H: BuildHasher> {
    marker: PhantomData<fn() -> HashSet<K, H>>,
}

impl<K, H> HashSetVisitor<K, H>
where
    K: Eq + Hash + Sync,
    H: BuildHasher,
{
    fn new() -> Self {
        HashSetVisitor {
            marker: PhantomData,
        }
    }
}

impl<'d, K, H> Visitor<'d> for HashSetVisitor<K, H>
where
    K: Deserialize<'d> + Eq + Hash + Sync,
    H: BuildHasher + Default,
{
    type Value = HashSet<K, H>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("HashSet")
    }

    fn visit_seq<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: SeqAccess<'d>,
    {
        let hashset =
            HashSet::with_capacity_and_hasher(access.size_hint().unwrap_or(0), Default::default());
        while let Some(key) = access.next_element()? {
            let _result = hashset.insert(key);
        }
        Ok(hashset)
    }
}

impl<'d, K, H> Deserialize<'d> for HashSet<K, H>
where
    K: Deserialize<'d> + Eq + Hash + Sync,
    H: BuildHasher + Default,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        deserializer.deserialize_seq(HashSetVisitor::<K, H>::new())
    }
}

impl<K, H> Serialize for HashSet<K, H>
where
    K: Eq + Hash + Serialize + Sync,
    H: BuildHasher,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        let mut error = None;
        self.for_each(|k| {
            if error.is_none() {
                if let Err(e) = seq.serialize_element(k) {
                    error.replace(e);
                }
            }
        });
        if let Some(e) = error {
            return Err(e);
        }
        seq.end()
    }
}

/// Helper type to allow `serde` to access [`HashIndex`] entries.
pub struct HashIndexVisitor<
    K: 'static + Clone + Eq + Hash + Sync,
    V: 'static + Clone + Sync,
    H: BuildHasher,
> {
    #[allow(clippy::type_complexity)]
    marker: PhantomData<fn() -> HashIndex<K, V, H>>,
}

impl<K, V, H> HashIndexVisitor<K, V, H>
where
    K: Clone + Eq + Hash + Sync,
    V: Clone + Sync,
    H: BuildHasher,
{
    fn new() -> Self {
        HashIndexVisitor {
            marker: PhantomData,
        }
    }
}

impl<'d, K, V, H> Visitor<'d> for HashIndexVisitor<K, V, H>
where
    K: Clone + Deserialize<'d> + Eq + Hash + Sync,
    V: Clone + Deserialize<'d> + Sync,
    H: BuildHasher + Default,
{
    type Value = HashIndex<K, V, H>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("HashIndex")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'d>,
    {
        let hashindex =
            HashIndex::with_capacity_and_hasher(access.size_hint().unwrap_or(0), H::default());
        while let Some((key, val)) = access.next_entry()? {
            let _result = hashindex.insert(key, val);
        }
        Ok(hashindex)
    }
}

impl<'d, K, V, H> Deserialize<'d> for HashIndex<K, V, H>
where
    K: Clone + Deserialize<'d> + Eq + Hash + Sync,
    V: Clone + Deserialize<'d> + Sync,
    H: BuildHasher + Default,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        deserializer.deserialize_map(HashIndexVisitor::<K, V, H>::new())
    }
}

impl<K, V, H> Serialize for HashIndex<K, V, H>
where
    K: Clone + Eq + Hash + Serialize + Sync,
    V: Clone + Serialize + Sync,
    H: 'static + BuildHasher,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        let mut error = None;
        self.iter(&Barrier::new()).any(|(k, v)| {
            if let Err(e) = map.serialize_entry(k, v) {
                error.replace(e);
                true
            } else {
                false
            }
        });
        if let Some(e) = error {
            return Err(e);
        }
        map.end()
    }
}

/// Helper type to allow `serde` to access [`TreeIndex`] entries.
pub struct TreeIndexVisitor<K: 'static + Clone + Ord + Sync, V: 'static + Clone + Sync> {
    #[allow(clippy::type_complexity)]
    marker: PhantomData<fn() -> TreeIndex<K, V>>,
}

impl<K, V> TreeIndexVisitor<K, V>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync,
{
    fn new() -> Self {
        TreeIndexVisitor {
            marker: PhantomData,
        }
    }
}

impl<'d, K, V> Visitor<'d> for TreeIndexVisitor<K, V>
where
    K: Clone + Deserialize<'d> + Ord + Sync,
    V: Clone + Deserialize<'d> + Sync,
{
    type Value = TreeIndex<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("TreeIndex")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'d>,
    {
        let treeindex = TreeIndex::default();
        while let Some((key, val)) = access.next_entry()? {
            let _result = treeindex.insert(key, val);
        }
        Ok(treeindex)
    }
}

impl<'d, K, V> Deserialize<'d> for TreeIndex<K, V>
where
    K: Clone + Deserialize<'d> + Ord + Sync,
    V: Clone + Deserialize<'d> + Sync,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        deserializer.deserialize_map(TreeIndexVisitor::<K, V>::new())
    }
}

impl<K, V> Serialize for TreeIndex<K, V>
where
    K: Clone + Ord + Serialize + Sync,
    V: Clone + Serialize + Sync,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        let mut error = None;
        self.iter(&Barrier::new()).any(|(k, v)| {
            if let Err(e) = map.serialize_entry(k, v) {
                error.replace(e);
                true
            } else {
                false
            }
        });
        if let Some(e) = error {
            return Err(e);
        }
        map.end()
    }
}
