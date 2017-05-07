package com.kybernetikos.jbuzztree.apis;

import com.kybernetikos.jbuzztree.data.Node;

import java.util.WeakHashMap;

public class MemoryApi<Key, Value> implements Api<Key, Value, Integer> {
    private int nextId = 0;
    private final WeakHashMap<Integer, Node<Key, Value, Integer>> cache = new WeakHashMap<>();

    @Override
    public Integer create(Node<Key, Value, Integer> node) {
        int id = nextId++;
        cache.put(id, node);
        node.setRef(id);
        return id;
    }

    @Override
    public void update(Integer ref, Node<Key, Value, Integer> node) {
        cache.put(ref, node);
        node.setRef(ref);
    }

    @Override
    public Node<Key, Value, Integer> read(Integer ref) {
        Node<Key, Value, Integer> result = cache.get(ref);
        if (result == null) {
            throw new IllegalStateException("Unable to find storable for id " + ref);
        }
        result.setRef(ref);
        return result;
    }

    @Override
    public void remove(Integer ref) {
        cache.remove(ref);
    }
}
