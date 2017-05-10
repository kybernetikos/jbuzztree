package com.kybernetikos.jbuzztree.apis;

import com.kybernetikos.jbuzztree.data.Node;

public interface Storage<Key, Value, NodeRef> {
    NodeRef create(Node<Key, Value, NodeRef> node);
    void update(NodeRef ref, Node<Key, Value, NodeRef> node);
    Node<Key, Value, NodeRef> read(NodeRef ref);
    void remove(NodeRef ref);
}
