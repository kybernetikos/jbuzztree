package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Storage;

import java.util.*;

public class Bucket<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private static boolean DEBUG = true;

    private List<Key> keys;
    private List<Value> values;
    private NodeRef next;
    private NodeRef previous;

    Bucket(List<Key> keys, List<Value> values, NodeRef next, NodeRef previous) {
        this.keys = new ArrayList<>(keys);
        this.values = new ArrayList<>(values);
        this.next = next;
        this.previous = previous;
    }

    @Override
    public Value get(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, Key key, Value defaultValue) {
        if (DEBUG) System.out.println("Bucket: Get " + key + " from bucket " + getRef());

        int index = Collections.binarySearch(keys, key, comparator);
        if (index < 0) {
            return defaultValue;
        }
        return values.get(index);
    }

    @Override
    public void put(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key, Value value) {
        if (DEBUG) System.out.println("Bucket: Put " + key + " = " + value.toString() + " to bucket " + getRef());

        int index = Collections.binarySearch(keys, key, comparator);
        if (index >= 0) {
            if (values.get(index) != value) {
                values.set(index, value);
                store(storage);
            }
        } else {
            int insertionPoint = -index - 1;
            keys.add(insertionPoint, key);
            values.add(insertionPoint, value);
            store(storage);
        }
    }

    @Override
    public Node<Key, Value, NodeRef> remove(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key) {
        if (DEBUG) System.out.println("Bucket: remove " + key + " from bucket " + getRef());

        int index = Collections.binarySearch(keys, key, comparator);
        if (index >= 0) {
            keys.remove(index);
            values.remove(index);
        }

        store(storage);
        return this;
    }

    // internal methods to support the api methods

    @Override
    boolean isOverful(int maxNodeChildren) {
        return values.size() > (maxNodeChildren - 1);
    }

    @Override
    boolean isUnderful(int maxNodeChildren) {
        return values.size() < Math.ceil(maxNodeChildren / 2) - 1;
    }

    @Override
    InternalNode<Key, Value, NodeRef> split(Storage<Key, Value, NodeRef> storage) {
        Bucket<Key, Value, NodeRef> newSibling = new Bucket<>(Collections.emptyList(), Collections.emptyList(), next, getRef());
        newSibling.store(storage);

        if (DEBUG) System.out.println("Bucket: splitting bucket " + getRef() + " into " + newSibling.getRef());

        Key newSplitKey = rebalanceSplitPoint(0, storage, null, newSibling);

        if (next != null) {
            Bucket<Key, Value, NodeRef> neighbour = (Bucket<Key, Value, NodeRef>) storage.read(next);
            neighbour.previous = newSibling.getRef();
            neighbour.store(storage);
        }

        next = newSibling.getRef();
        store(storage);

        return new InternalNode<>(Collections.singletonList(newSplitKey), Arrays.asList(getRef(), newSibling.getRef()));
    }

    @Override
    void mergeWith(Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        Bucket<Key, Value, NodeRef> right = (Bucket<Key, Value, NodeRef>) rightSibling;

        keys.addAll(right.keys);
        values.addAll(right.values);
        next = ((Bucket<Key, Value, NodeRef>) rightSibling).next;
        store(storage);

        if (next != null) {
            Bucket<Key, Value, NodeRef> nextBucket = ((Bucket<Key, Value, NodeRef>) storage.read(next));
            nextBucket.previous = getRef();
            nextBucket.store(storage);
        }

        storage.remove(rightSibling.getRef());
    }

    @Override
    Key rebalanceSplitPoint(int maxNodeChildren, Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        Bucket<Key, Value, NodeRef> right = (Bucket<Key, Value, NodeRef>) rightSibling;

        int totalValues = values.size() + right.values.size();
        if (maxNodeChildren > 0) {
            int minAcrossBoth = 2 * ((int) Math.ceil(maxNodeChildren / 2) - 1);

            if (totalValues < minAcrossBoth) {
                return null;
            }
        }

        List<Key> allKeys = new ArrayList<>(totalValues);
        allKeys.addAll(keys);
        allKeys.addAll(right.keys);
        List<Value> allValues = new ArrayList<>(totalValues);
        allValues.addAll(values);
        allValues.addAll(right.values);

        int newSplitIndex = (int) Math.floor(allKeys.size() / 2);

        right.keys = new ArrayList<>(allKeys.subList(newSplitIndex, allKeys.size()));
        right.values = new ArrayList<>(allValues.subList(newSplitIndex, allValues.size()));
        right.store(storage);

        keys = new ArrayList(allKeys.subList(0, newSplitIndex));
        values = new ArrayList(allValues.subList(0, newSplitIndex));
        store(storage);

        return allKeys.get(newSplitIndex);
    }

    @Override
    void delete(Storage<Key, Value, NodeRef> storage) {
        if (previous != null) {
            Bucket<Key, Value, NodeRef> previousBucket = (Bucket) storage.read(previous);
            previousBucket.next = next;
            previousBucket.store(storage);
        }
        if (next != null) {
            Bucket<Key, Value, NodeRef> nextBucket = (Bucket) storage.read(next);
            nextBucket.previous = previous;
            nextBucket.store(storage);
        }
        storage.remove(getRef());
    }

    @Override
    boolean isEmpty() {
        return values.isEmpty();
    }

    // accessors for storage providers.

    public List<Key> getKeys() {
        return keys;
    }

    public List<Value> getValues() {
        return values;
    }

    public NodeRef getNext() {
        return next;
    }

    public NodeRef getPrevious() {
        return previous;
    }

    // display methods

    public String toString() {
        return keys.toString() + "->" + values.toString() + "\t" + next + " " + previous;
    }

    @Override
    public String dump(Storage<Key, Value, NodeRef> storage, String prefix) {
        StringBuilder result = new StringBuilder();
        result.append(prefix).append("+ <-- ").append(previous).append("\n");
        for (int i = 0; i < this.values.size(); ++i) {
            result.append(prefix).append("+ ").append(keys.get(i)).append(" -> ").append(values.get(i)).append("\n");
        }
        result.append(prefix).append("+ --> ").append(next).append("\n");
        return result.toString();
    }
}
