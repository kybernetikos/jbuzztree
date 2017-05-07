package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Api;

import java.util.*;

public class Bucket<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private List<Key> keys;
    private List<Value> values;
    private NodeRef next;
    private NodeRef previous;

    public Bucket(List<Key> keys, List<Value> values, NodeRef next, NodeRef previous) {
        this.keys = new ArrayList<>(keys);
        this.values = new ArrayList<>(values);
        this.next = next;
        this.previous = previous;
    }

    @Override
    public boolean isOverful(int maxNodeChildren) {
        return values.size() > (maxNodeChildren - 1);
    }

    @Override
    public Value get(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, Key key, Value defaultValue) {
        int index = Collections.binarySearch(keys, key, comparator);
        if (index < 0) {
            return defaultValue;
        }
        return values.get(index);
    }

    @Override
    public void set(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, int maxNodeChildren, Key key, Value value) {
        int index = Collections.binarySearch(keys, key, comparator);
        if (index >= 0) {
            if (values.get(index) != value) {
                values.set(index, value);
                store(api);
            }
        } else {
            int insertionPoint = -index - 1;
            keys.add(insertionPoint, key);
            values.add(insertionPoint, value);
            store(api);
        }
    }

    public InternalNode<Key, Value, NodeRef> split(Api<Key, Value, NodeRef> api) {
        int newSplitIndex = (int) Math.floor(keys.size() / 2);
        List<Key> allKeys = keys;
        List<Value> allValues = values;
        Bucket<Key, Value, NodeRef> newSibling = new Bucket<>(allKeys.subList(newSplitIndex, allKeys.size()), allValues.subList(newSplitIndex, allValues.size()), next, this.getRef());
        newSibling.store(api);

        if (next != null) {
            Bucket<Key, Value, NodeRef> neighbour = (Bucket<Key, Value, NodeRef>) api.read(next);
            neighbour.previous = newSibling.getRef();
            neighbour.store(api);
        }

        keys = new ArrayList(allKeys.subList(0, newSplitIndex));
        values = new ArrayList(allValues.subList(0, newSplitIndex));
        next = newSibling.getRef();
        store(api);

        return new InternalNode<>(Arrays.asList(allKeys.get(newSplitIndex)), Arrays.asList(getRef(), newSibling.getRef()));
    }

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

    public String toString() {
        return keys.toString() + "->" + values.toString() + "\t" + next + " " + previous;
    }
}
