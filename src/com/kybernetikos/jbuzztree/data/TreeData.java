package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Storage;

import java.util.Collections;
import java.util.Comparator;

public class TreeData<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private transient Node<Key, Value, NodeRef> root;
    private NodeRef rootRef;

    public TreeData(Storage<Key, Value, NodeRef> storage, NodeRef rootRef) {
        if (rootRef == null) {
            root = new Bucket<>(Collections.emptyList(), Collections.emptyList(), null, null);
            root.store(storage);
            this.rootRef = root.getRef();
        } else {
            this.rootRef = rootRef;
            ensureRoot(storage);
            store(storage);
        }
    }

    @Override
    public Value get(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, Key key, Value defaultValue) {
        return ensureRoot(storage).get(comparator, storage, key, defaultValue);
    }

    @Override
    public void put(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key, Value value) {
        ensureRoot(storage).put(comparator, storage, maxNodeChildren, key, value);
        if (root.isOverful(maxNodeChildren)) {
            root = root.split(storage);
            root.store(storage);
            rootRef = root.getRef();
            store(storage);
        }
    }

    @Override
    public Node<Key, Value, NodeRef> remove(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key) {
        Node<Key, Value, NodeRef> result = ensureRoot(storage).remove(comparator, storage, maxNodeChildren, key);
        if (result != root) {
            root = result;
            store(storage);
        }
        return null;
    }

    // internal api

    @Override
    boolean isOverful(int maxNodeChildren) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    boolean isUnderful(int maxNodeChildren) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    InternalNode<Key, Value, NodeRef> split(Storage<Key, Value, NodeRef> storage) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    Key rebalanceSplitPoint(int maxNodeChildren, Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    void mergeWith(Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    void delete(Storage<Key, Value, NodeRef> storage) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    public boolean isEmpty() {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    // private api

    private Node<Key, Value, NodeRef> ensureRoot(Storage<Key, Value, NodeRef> storage) {
        if (root == null) {
            root = storage.read(rootRef);
        }
        return root;
    }

    // display methods

    public String toString() {
        return "Root: " + rootRef + " " + String.valueOf(root);
    }

    @Override
    public String dump(Storage<Key, Value, NodeRef> storage, String prefix) {
        return ("(" + getRef() + ") -> (") + rootRef + ")\n" +
                ensureRoot(storage).dump(storage, "        ");
    }
}
