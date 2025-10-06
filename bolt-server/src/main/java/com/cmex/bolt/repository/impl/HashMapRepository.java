package com.cmex.bolt.repository.impl;


import com.cmex.bolt.repository.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 业务串行执行、无并发场景
 */
public abstract class HashMapRepository<K, V> implements Repository<K, V> {

    protected final HashMap<K, V> holder;

    public HashMapRepository() {
        holder = new HashMap<>();
    }

    public V getOrCreate(K id, V value) {
        return holder.computeIfAbsent(id, k -> value);
    }

    public Optional<V> get(K id) {
        return Optional.ofNullable(holder.get(id));
    }

    public V remove(K id) {
        return holder.remove(id);
    }

    public boolean exists(K id) {
        return holder.containsKey(id);
    }

    @Override
    public Map<K, V> getAllData() {
        return holder;
    }

}
