package dbms.storage;

import java.util.HashMap;

/**
 * Created by alex on 12.03.17.
 */
public class BufferedStorage<K, V> {
    private HashMap<K, V> buffer = new HashMap<K, V>();
    private Integer limit = 4096; // TODO: choose more intellegent constant

    public void add(K key, V value) {
        buffer.put(key, value);
        if(buffer.size() > limit) {
            for(K k : buffer.keySet()) {
                if(!k.equals(key)) {
                    buffer.remove(k);
                }
            }
        }
    }

    public V get(K key) {
        return buffer.get(key);
    }

    public boolean contains(K key) {
        return buffer.containsKey(key);
    }
}
