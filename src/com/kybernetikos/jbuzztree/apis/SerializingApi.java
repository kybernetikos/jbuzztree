package com.kybernetikos.jbuzztree.apis;

import com.kybernetikos.jbuzztree.data.Node;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SerializingApi<Key, Value> implements Api<Key, Value, Integer> {

    Map<Integer, byte[]> cache;
    int nextRef = 0;

    public SerializingApi() {
        cache = new HashMap<>();
    }

    byte[] serialize(Node<Key, Value, Integer> node) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(out).writeObject(node);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return out.toByteArray();
    }

    Node<Key, Value, Integer> deserialize(byte[] data) {
        try {
            return (Node<Key, Value, Integer>) new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public java.lang.Integer create(Node<Key, Value, Integer> node) {
        int ref = nextRef++;
        cache.put(ref, serialize(node));
        return ref;
    }

    @Override
    public void update(Integer ref, Node<Key, Value, Integer> node) {
        cache.put(ref, serialize(node));
    }

    @Override
    public Node<Key, Value, Integer> read(Integer ref) {
        if (!cache.containsKey(ref)) {
            throw new IllegalStateException("Unable to find object ref " + ref);
        }
        return deserialize(cache.get(ref));
    }

    @Override
    public void remove(Integer ref) {
        cache.remove(ref);
    }
}
