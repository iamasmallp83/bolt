package com.cmex.bolt.repository;

import java.util.Map;
import java.util.Optional;

public interface Repository<K, V> {

    V getOrCreate(K id, V value);

    boolean exists(K id);

    Optional<V> get(K id);

    V remove(K id);

    Map<K, V> getAllData();

}