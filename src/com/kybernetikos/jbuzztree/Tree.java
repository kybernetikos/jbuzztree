package com.kybernetikos.jbuzztree;

import com.kybernetikos.jbuzztree.apis.Storage;
import com.kybernetikos.jbuzztree.data.TreeData;

import java.util.Comparator;

public class Tree<Key, Value, NodeRef> {
    private Comparator<Key> comparator;
    private Storage<Key, Value, NodeRef> storage;
    private int maxNodeChildren;

    private TreeData<Key, Value, NodeRef> tree;

    Tree(NodeRef treeRef, Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren) {
        if (maxNodeChildren < 2) {
            throw new IllegalArgumentException("MaxNodeChildren must be at least 2, was " + maxNodeChildren);
        }
        this.comparator = comparator;
        this.storage = storage;
        this.maxNodeChildren = maxNodeChildren;

        if (treeRef == null) {
            tree = new TreeData<>(storage, null);
        } else {
            tree = (TreeData<Key, Value, NodeRef>) storage.read(treeRef);
        }
    }

    void put(Key key, Value value) {
        tree.put(comparator, storage, maxNodeChildren, key, value);
    }

    Value get(Key key, Value defaultValue) {
        return tree.get(comparator, storage, key, defaultValue);
    }

    void remove(Key key) {
        tree.remove(comparator, storage, maxNodeChildren, key);
    }

    NodeRef getRef() {
        return tree.getRef();
    }

    @Override
    public String toString() {
        return tree.dump(storage, null);
    }
}
