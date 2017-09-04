/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Last Recently used policy cache.
 * @author hrishi
 * @param <K> Type of the Cache Key
 * @param <V> Type of the Cache Elements
 */
public class LRUCache<K, V> {
    
    private int capacity;
    private LinkedHashMap<K, V> map;

    /**
     * Consructor
     * 
     * @param capacity The maximum size of this Cache
     */
    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<K, V>();
    }
    
    /**
     * @param key
     * @return True if Cache contains key k
     */
    public boolean containsKey(K key){
        return map.containsKey(key);
    }
    
    /**
     * Gets Element corresponding to key
     * @param key
     * @return The Element
     */
    public V get(K key) {
        V value = this.map.get(key);
        if (value != null) {
            this.set(key, value);
        }
        return value;
    }
    
    /**
     * Evicts the least recently used element
     * @return The evicted element
     */
    public V evict(){
        Iterator<K> it = this.map.keySet().iterator();
        K k = it.next();
        V v = map.get(k);
        it.remove();
        return v;
    }

    /**
     * Puts element into the cache
     * @param key Key of the element
     * @param value The element
     */
    public void set(K key, V value) {
        if (this.map.containsKey(key)) {
            this.map.remove(key);
        } else if (this.map.size() == this.capacity) {
            this.evict();
        }
        map.put(key, value);
    }
    
    /**
     * @return The number of elements in cache
     */
    public int size(){
        return map.size();
    }
    
    /**
     * @return The iterator to key set of this cache
     */
    public Iterator<K> iterator(){
        return this.map.keySet().iterator();
    }
    
    /**
     * Removes element with given key
     * @param key The key of element to be removed
     */
    public void remove(K key){
        this.map.remove(key);
    }
    
}
