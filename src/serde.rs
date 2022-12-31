use super::HashMap;

use serde::de::{Deserialize, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::Deserializer;

use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

pub struct HashMapVisitor<K: 'static + Eq + Hash + Sync, V: 'static + Sync, S: BuildHasher> {
    #[allow(clippy::type_complexity)]
    marker: PhantomData<fn() -> HashMap<K, V, S>>,
}

impl<K, V, S> HashMapVisitor<K, V, S>
where
    K: Eq + Hash + Sync,
    V: Sync,
    S: BuildHasher,
{
    fn new() -> Self {
        HashMapVisitor {
            marker: PhantomData,
        }
    }
}

impl<'de, K, V, S> Visitor<'de> for HashMapVisitor<K, V, S>
where
    K: Deserialize<'de> + Eq + Hash + Sync,
    V: Deserialize<'de> + Sync,
    S: BuildHasher + Default,
{
    type Value = HashMap<K, V, S>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a HashMap")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let map = HashMap::with_capacity_and_hasher(access.size_hint().unwrap_or(0), S::default());

        while let Some((key, value)) = access.next_entry()? {
            map.upsert(key, || value, |_, _| ());
        }

        Ok(map)
    }
}

impl<'de, K, V, S> Deserialize<'de> for HashMap<K, V, S>
where
    K: Deserialize<'de> + Eq + Hash + Sync,
    V: Deserialize<'de> + Sync,
    S: BuildHasher + Default,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(HashMapVisitor::<K, V, S>::new())
    }
}

impl<K, V, H> Serialize for HashMap<K, V, H>
where
    K: Serialize + Eq + Hash + Sync,
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

#[cfg(test)]
mod serde_test {
    use crate::HashMap;

    use serde_test::{assert_tokens, Token};

    #[test]
    fn serde_hashmap() {
        let map: HashMap<u64, i16> = HashMap::new();
        assert!(map.insert(2, -6).is_ok());
        assert_tokens(
            &map,
            &[
                Token::Map { len: Some(1) },
                Token::U64(2),
                Token::I16(-6),
                Token::MapEnd,
            ],
        );
    }
}
