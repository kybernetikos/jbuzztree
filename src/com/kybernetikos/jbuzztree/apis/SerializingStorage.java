package com.kybernetikos.jbuzztree.apis;

import com.kybernetikos.jbuzztree.data.Node;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SerializingStorage<Key, Value> implements Storage<Key, Value, Integer> {

    private static boolean DEBUG = true;

    private Map<Integer, byte[]> cache;
    private int nextRef = 0;

    public SerializingStorage() {
        cache = new HashMap<>();
    }

    @Override
    public java.lang.Integer create(Node<Key, Value, Integer> node) {
        int ref = nextRef++;
        if (DEBUG) System.out.println("API: Create " + ref + " " + node.toString());
        cache.put(ref, serialize(node));
        return ref;
    }

    @Override
    public void update(Integer ref, Node<Key, Value, Integer> node) {
        if (DEBUG) System.out.println("API: Update " + ref + " " + node.toString());
        cache.put(ref, serialize(node));
    }

    @Override
    public Node<Key, Value, Integer> read(Integer ref) {
        if (!cache.containsKey(ref)) {
            throw new IllegalStateException("Unable to find object ref " + ref);
        }
        Node<Key, Value, Integer> result = deserialize(cache.get(ref));
        result.setRef(ref);

        if (DEBUG) System.out.println("API: Read " + ref + " " +result.toString());
        return result;
    }

    @Override
    public void remove(Integer ref) {
        if (DEBUG) System.out.println("API: Remove " + ref);
        cache.remove(ref);
    }

    private byte[] serialize(Node<Key, Value, Integer> node) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(out).writeObject(node);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return out.toByteArray();
    }

    private Node<Key, Value, Integer> deserialize(byte[] data) {
        try {
            return (Node<Key, Value, Integer>) new ObjectInputStream(new ByteArrayInputStream(data)).readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}