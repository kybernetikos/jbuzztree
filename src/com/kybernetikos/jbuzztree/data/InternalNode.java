package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Api;

import java.util.*;

public class InternalNode<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private List<Key> splits;
    private List<NodeRef> children;

    public InternalNode(List<Key> splits, List<NodeRef> children) {
        if (splits.size() != children.size() - 1) {
            throw new RuntimeException("The splits must be just 1 length less than the children, they were " + splits.size() + " " +children.size());
        }
        this.splits = new ArrayList<>(splits);
        this.children = new ArrayList<>(children);
    }

    @Override
    public boolean isOverful(int maxNodeChildren) {
        return this.children.size() > maxNodeChildren;
    }

    @Override
    public InternalNode<Key, Value, NodeRef> split(Api<Key, Value, NodeRef> api) {
        int newSplitIndex = (int) Math.floor(splits.size() / 2);
        List<Key> allSplits = new ArrayList<>(splits);
        List<NodeRef> allChildren = new ArrayList<>(children);
        InternalNode<Key, Value, NodeRef> newSibling = new InternalNode<>(allSplits.subList(newSplitIndex + 1, allSplits.size()), allChildren.subList(newSplitIndex + 1, allChildren.size()));
        newSibling.store(api);

        splits = allSplits.subList(0, newSplitIndex);
        children = allChildren.subList(0, newSplitIndex + 1);
        store(api);

        return new InternalNode<>(Collections.singletonList(allSplits.get(newSplitIndex)), Arrays.asList(getRef(), newSibling.getRef()));
    }

    public Value get(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, Key key, Value defaultValue) {
        int index = findChildIndex(comparator, key);
        return readChild(api, index).get(comparator, api, key, defaultValue);
    }

    @Override
    public void set(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, int maxNodeChildren, Key key, Value value) {
        int index = findChildIndex(comparator, key);
        Node<Key, Value, NodeRef> child = readChild(api, index);
        child.set(comparator, api, maxNodeChildren, key, value);

        if (child.isOverful(maxNodeChildren)) {
            InternalNode<Key, Value, NodeRef> partialResult = child.split(api);
            mergeInsertionRemainder(comparator, api, partialResult);
        }
    }

    private void mergeInsertionRemainder(Comparator<Key> comparator, Api<Key, Value, NodeRef> api, InternalNode<Key, Value, NodeRef> partialResult) {
        Key pivot = partialResult.splits.get(0);
        NodeRef left = partialResult.children.get(0);
        NodeRef right = partialResult.children.get(1);
        int index = Collections.binarySearch(splits, pivot, comparator);
        splits.add(-index-1, pivot);
        children.set(-index-1, left);
        children.add(-index, right);
        store(api);
    }

    private int findChildIndex(Comparator<Key> comparator, Key key) {
        int index = Collections.binarySearch(splits, key, comparator);
        boolean foundExactMatch = index >= 0;
        int rightSubtreeIndex = index + 1;
        int leftSubtreeIndex = -index - 1;
        return foundExactMatch ? rightSubtreeIndex : leftSubtreeIndex;
    }

    private Node<Key, Value, NodeRef> readChild(Api<Key, Value, NodeRef> api, int index) {
        return (Node<Key, Value, NodeRef>) api.read(children.get(index));
    }

    public List<Key> getSplits() {
        return splits;
    }

    public List<NodeRef> getChildren() {
        return children;
    }

    public String toString() {
        return splits.toString() + "->" + children.toString();
    }

}
