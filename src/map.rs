use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
};

use dashmap::DashMap;

pub(crate) struct MapGroup<K, V> {
    group: Box<[DashMap<K, V>]>,
    capacity: usize,
}

#[allow(dead_code)]
impl<K: Eq + Hash, V> MapGroup<K, V> {
    pub fn new(capcity: usize) -> Self {
        let real = next_power_of_two(capcity);
        let mut group_uninit = Box::<[DashMap<K, V>]>::new_uninit_slice(real);
        MapGroup {
            group: unsafe {
                for i in 0..real {
                    group_uninit[i].as_mut_ptr().write(DashMap::new());
                }
                group_uninit.assume_init()
            },
            capacity: real,
        }
    }

    pub fn get<'a, Q>(&'a self, key: &Q) -> Option<dashmap::mapref::one::Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let shard = self.compute_slot(key);
        self.group[shard].get(key)
    }

    pub fn get_mut<'a, Q>(&'a self, key: &Q) -> Option<dashmap::mapref::one::RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let shard = self.compute_slot(key);
        self.group[shard].get_mut(key)
    }

    pub fn insert<Q>(&self, key: K, val: V) -> Option<V> 
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let shard = self.compute_slot(key.borrow());
        self.group[shard].insert(key, val)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)> 
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let shard = self.compute_slot(key);
        self.group[shard].remove(key)
    }

    fn compute_slot<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize & (self.capacity - 1)) as _
    }
}

fn next_power_of_two(mut n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n -= 1;

    while n & (n - 1) > 0 {
        n &= n - 1;
    }

    n <<= 1;
    n
}
