package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Storage;

import java.io.Serializable;
import java.util.Comparator;

public abstract class Node<Key, Value, NodeRef> implements Serializable {
    // public api

    public abstract Value get(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, Key key, Value defaultValue);

    public abstract void put(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key, Value value);

    public abstract Node<Key, Value, NodeRef> remove(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key);

    // internal api

    abstract boolean isOverful(int maxNodeChildren);
    abstract boolean isUnderful(int maxNodeChildren);
    abstract InternalNode<Key, Value, NodeRef> split(Storage<Key, Value, NodeRef> storage);
    abstract Key rebalanceSplitPoint(int maxNodeChildren, Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling);
    abstract void mergeWith(Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling);
    abstract public String dump(Storage<Key, Value, NodeRef> storage, String prefix);
    abstract void delete(Storage<Key, Value, NodeRef> storage);
    abstract boolean isEmpty();

    // storage related

    private transient NodeRef ref = null;

    void store(Storage<Key, Value, NodeRef> storage) {
        NodeRef ref = getRef();
        if (ref == null) {
            setRef(storage.create(this));
        } else {
            storage.update(ref, this);
        }
    }

    public NodeRef getRef() {
        return ref;
    }

    public void setRef(NodeRef ref) {
        this.ref = ref;
    }

}