package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Api;

import java.io.Serializable;
import java.util.Comparator;

public abstract class Node<Key, Value, NodeRef> implements Serializable {
    private transient NodeRef ref = null;

    public void store(Api<Key, Value, NodeRef> api) {
        NodeRef ref =  getRef();
        if (ref == null) {
            setRef(api.create(this));
        } else {
            api.update(ref, this);
        }
    }

    public NodeRef getRef() {
        return ref;
    }
    public void setRef(NodeRef ref) {
        this.ref = ref;
    }

    public abstract boolean isOverful(int maxNodeChildren);
    public abstract InternalNode<Key, Value, NodeRef> split(Api<Key, Value, NodeRef> api);

    public abstract Value get(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, Key key, Value defaultValue);
    public abstract void set(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, int maxNodeChildren, Key key, Value value);
}