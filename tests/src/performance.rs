use scc::HashMap;

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::RandomState;

    #[test]
    fn basic_latency() {
        let hashmap: HashMap<u64, u32, RandomState> = HashMap::new(RandomState::new(), Some(10));
    }
}
