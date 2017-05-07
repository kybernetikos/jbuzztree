package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Api;

import java.util.Collections;
import java.util.Comparator;

public class TreeData<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private transient Node<Key, Value, NodeRef> root;
    private NodeRef rootRef;

    public TreeData(Api<Key, Value, NodeRef> api, NodeRef rootRef) {
        if (rootRef == null) {
            root = new Bucket<>(Collections.emptyList(), Collections.emptyList(), null, null);
            root.store(api);
            this.rootRef = root.getRef();
        } else {
            this.rootRef = rootRef;
            ensureRoot(api);
            store(api);
        }
    }

    private Node<Key, Value, NodeRef> ensureRoot(Api<Key, Value, NodeRef> api) {
        if (root == null) {
            root = api.read(rootRef);
        }
        return root;
    }

    @Override
    public boolean isOverful(int maxNodeChildren) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    public InternalNode<Key, Value, NodeRef> split(Api<Key, Value, NodeRef> api) {
        throw new IllegalStateException("This is just a pointer to the root node.");
    }

    @Override
    public Value get(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, Key key, Value defaultValue) {
        return ensureRoot(api).get(comparator, api, key, defaultValue);
    }

    @Override
    public void set(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, int maxNodeChildren, Key key, Value value) {
        ensureRoot(api).set(comparator, api, maxNodeChildren, key, value);
        if (root.isOverful(maxNodeChildren)) {
            root = root.split(api);
            root.store(api);
            rootRef = root.getRef();
            store(api);
        }
    }

    public String toString() {
        return "Root: " + rootRef + " " + String.valueOf(root);
    }
}
