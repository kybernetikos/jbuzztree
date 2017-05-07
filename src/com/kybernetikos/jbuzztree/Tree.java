package com.kybernetikos.jbuzztree;

import com.kybernetikos.jbuzztree.apis.Api;
import com.kybernetikos.jbuzztree.data.TreeData;

import java.util.Comparator;

public class Tree<Key, Value, NodeRef> {
    private Comparator<Key> comparator;
    private Api<Key, Value, NodeRef> api;
    private int maxNodeChildren;

    private TreeData<Key, Value, NodeRef> tree;

    public Tree(NodeRef treeRef, Comparator<Key> comparator, Api<Key, Value, NodeRef> api, int maxNodeChildren) {
        if (maxNodeChildren < 2) {
            throw new IllegalArgumentException("MaxNodeChildren must be at least 2, was " + maxNodeChildren);
        }
        this.comparator = comparator;
        this.api = api;
        this.maxNodeChildren = maxNodeChildren;

        if (treeRef == null) {
            tree = new TreeData<>(api, null);
        } else {
            tree = (TreeData<Key, Value, NodeRef>) api.read(treeRef);
        }
    }

    public void put(Key key, Value value) {
        tree.set(comparator, api, maxNodeChildren, key, value);
    }

    public Value get(Key key, Value defaultValue) {
        return tree.get(comparator, api, key, defaultValue);
    }

    public NodeRef getRef() {
        return tree.getRef();
    }
}
