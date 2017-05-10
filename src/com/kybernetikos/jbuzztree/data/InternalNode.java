package com.kybernetikos.jbuzztree.data;

import com.kybernetikos.jbuzztree.apis.Storage;

import java.util.*;

public class InternalNode<Key, Value, NodeRef> extends Node<Key, Value, NodeRef> {
    private static boolean DEBUG = true;

    private List<Key> splits;
    private List<NodeRef> children;

    InternalNode(List<Key> splits, List<NodeRef> children) {
        if (children.size() != 0 && splits.size() != children.size() - 1) {
            throw new RuntimeException("The splits must be just 1 length less than the children, they were " + splits.size() + " " +children.size());
        }
        this.splits = new ArrayList<>(splits);
        this.children = new ArrayList<>(children);
    }

    @Override
    public Value get(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, Key key, Value defaultValue) {
        if (DEBUG) System.out.println("Node: getting " + key + " from " + getRef());

        int index = findChildIndex(comparator, key);
        return readChild(storage, index).get(comparator, storage, key, defaultValue);
    }

    @Override
    public void put(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key, Value value) {
        if (DEBUG) System.out.println("Node: putting " + key + " = " + value + " into " + getRef());

        int index = findChildIndex(comparator, key);
        Node<Key, Value, NodeRef> child = readChild(storage, index);
        child.put(comparator, storage, maxNodeChildren, key, value);

        if (child.isOverful(maxNodeChildren)) {
            InternalNode<Key, Value, NodeRef> partialResult = child.split(storage);
            mergeInsertionRemainder(comparator, storage, partialResult);
        }
    }

    @Override
    public Node<Key, Value, NodeRef> remove(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, int maxNodeChildren, Key key) {
        if (DEBUG) System.out.println("Node: removing " + key + " from " + getRef());

        // TODO: correct splits in the parents...

        boolean needsUpdate = false;

        int index = findChildIndex(comparator, key);
        Node<Key, Value, NodeRef> workingNode = readChild(storage, index);
        Node<Key, Value, NodeRef> child = workingNode.remove(comparator, storage, maxNodeChildren, key);
        if (child == null || child.isEmpty()) {
            if (child != null) {
                child.delete(storage);
            }
            children.remove(index);
            if (index < splits.size()) {
                splits.remove(index);
            } else {
                splits.remove(splits.size() - 1);
            }
            if (children.size() < 2) {
                delete(storage);
                if (children.isEmpty()) {
                    return null;
                }
                return storage.read(children.get(0));
            }
            store(storage);
            return this;
        }

        if (workingNode != child) {
            storage.remove(workingNode.getRef());
            this.children.set(index, child.getRef());
            needsUpdate = true;
        }

        if (child.isUnderful(maxNodeChildren) && index > 0) {
            Key newSplit = readChild(storage, index - 1).rebalanceSplitPoint(maxNodeChildren, storage, splits.get(index - 1), child);
            if (newSplit != null) {
                splits.set(index - 1, newSplit);
                needsUpdate = true;
            }
        }

        if (child.isUnderful(maxNodeChildren) && index < children.size() - 1) {
            Key newSplit = child.rebalanceSplitPoint(maxNodeChildren, storage, splits.get(index), readChild(storage, index + 1));
            if (newSplit != null) {
                splits.set(index, newSplit);
                needsUpdate = true;
            }
        }

        if (child.isUnderful(maxNodeChildren) && index < children.size() - 1) {
            child.mergeWith(storage, splits.get(index), readChild(storage, index+1));
        }

        if (child.isUnderful(maxNodeChildren) && index > 0) {
            readChild(storage, index-1).mergeWith(storage, splits.get(index - 1), child);
        }

        if (needsUpdate) {
            store(storage);
        }

        if (children.size() == 1) {
            child.store(storage);
            return child;
        }
        return this;
    }

    // internal methods to support api

    @Override
    boolean isOverful(int maxNodeChildren) {
        return this.children.size() > maxNodeChildren;
    }

    @Override
    boolean isUnderful(int maxNodeChildren) {
        return this.children.size() < Math.ceil(maxNodeChildren / 2);
    }

    @Override
    InternalNode<Key, Value, NodeRef> split(Storage<Key, Value, NodeRef> storage) {
        InternalNode<Key, Value, NodeRef> newSibling = new InternalNode<>(Collections.emptyList(), Collections.emptyList());
        newSibling.store(storage);

        Key newSplitKey = rebalanceSplitPoint(0, storage, null, newSibling);

        return new InternalNode<>(Collections.singletonList(newSplitKey), Arrays.asList(getRef(), newSibling.getRef()));
    }

    @Override
    Key rebalanceSplitPoint(int maxNodeChildren, Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        InternalNode<Key, Value, NodeRef> right = (InternalNode<Key, Value, NodeRef>) rightSibling;

        int totalChildren = children.size() + right.children.size();
        if (maxNodeChildren > 0) {
            int minAcrossBoth = 2 * (int) Math.ceil(maxNodeChildren / 2);

            if (totalChildren < minAcrossBoth) {
                return null;
            }
        }

        List<Key> allSplits = new ArrayList<>(totalChildren - 1);
        allSplits.addAll(splits);
        if (currentSplitPoint != null) {
            allSplits.add(currentSplitPoint);
        }
        allSplits.addAll(right.splits);
        List<NodeRef> allChildren = new ArrayList<>(totalChildren);
        allChildren.addAll(children);
        allChildren.addAll(right.children);

        int newSplitIndex = (int) Math.floor(allSplits.size() / 2);

        right.splits = new ArrayList<>(allSplits.subList(newSplitIndex + 1, allSplits.size()));
        right.children = new ArrayList<>(allChildren.subList(newSplitIndex + 1, allChildren.size()));
        right.store(storage);

        splits = new ArrayList(allSplits.subList(0, newSplitIndex));
        children = new ArrayList(allChildren.subList(0, newSplitIndex + 1));
        store(storage);

        return allSplits.get(newSplitIndex);
    }

    @Override
    void mergeWith(Storage<Key, Value, NodeRef> storage, Key currentSplitPoint, Node<Key, Value, NodeRef> rightSibling) {
        InternalNode<Key, Value, NodeRef> right = (InternalNode<Key, Value, NodeRef>) rightSibling;

        splits.add(currentSplitPoint);
        splits.addAll(right.splits);
        children.addAll(right.children);
        store(storage);

        storage.remove(rightSibling.getRef());
    }

    @Override
    void delete(Storage<Key, Value, NodeRef> storage) {
        storage.remove(getRef());
    }

    @Override
    boolean isEmpty() {
        return children.isEmpty();
    }

    // private methods

    private void mergeInsertionRemainder(Comparator<Key> comparator, Storage<Key, Value, NodeRef> storage, InternalNode<Key, Value, NodeRef> partialResult) {
        Key pivot = partialResult.splits.get(0);
        NodeRef left = partialResult.children.get(0);
        NodeRef right = partialResult.children.get(1);
        int index = Collections.binarySearch(splits, pivot, comparator);
        splits.add(-index-1, pivot);
        children.set(-index-1, left);
        children.add(-index, right);
        store(storage);
    }

    private int findChildIndex(Comparator<Key> comparator, Key key) {
        int index = Collections.binarySearch(splits, key, comparator);
        boolean foundExactMatch = index >= 0;
        int rightSubtreeIndex = index + 1;
        int leftSubtreeIndex = -index - 1;
        return foundExactMatch ? rightSubtreeIndex : leftSubtreeIndex;
    }

    private Node<Key, Value, NodeRef> readChild(Storage<Key, Value, NodeRef> storage, int index) {
        return storage.read(children.get(index));
    }

    // accessor methods for storage providers

    public List<Key> getSplits() {
        return splits;
    }

    public List<NodeRef> getChildren() {
        return children;
    }

    // display methods

    public String toString() {
        return splits.toString() + "->" + children.toString();
    }

    @Override
    public String dump(Storage<Key, Value, NodeRef> storage, String prefix) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < this.children.size(); ++i) {
            String connector = "|";
            if (i == this.children.size() - 1) {
                connector = " ";
            }
            result.append(prefix).append("|\n");
            result.append(prefix).append("+- (").append(this.children.get(i)).append(")\n");
            result.append(storage.read(this.children.get(i)).dump(storage, prefix + connector+ "   "));
            if (i < this.splits.size()) {
                result.append(prefix).append("|\n");
                result.append(prefix).append("| <").append(this.splits.get(i)).append(">\n");
            }
        }
        return result.toString();
    }
}
