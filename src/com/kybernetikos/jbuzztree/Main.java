package com.kybernetikos.jbuzztree;

import com.kybernetikos.jbuzztree.apis.Storage;
import com.kybernetikos.jbuzztree.apis.SerializingStorage;

import java.util.Comparator;

public class Main {

    public static void main(String[] args) {
        Storage<String, String, Integer> storage = new SerializingStorage<>();
        Tree<String, String, Integer> tree = new Tree<>(null, Comparator.naturalOrder(), storage, 3);

        tree.put("hello", "world");
        tree.put("a", "b");
        tree.put("c", "d");
        tree.put("e", "f");
        tree.put("bob", "fred");
        tree.put("cat", "dog");
        tree.put("b", "x");
        tree.put("d", "y");

        tree.remove("c");
        tree.remove("cat");
        tree.remove("dog");
        tree.remove("d");
        tree.remove("e");
        tree.remove("a");
        tree.remove("bob");
        tree.remove("b");
        tree.remove("hello");

        tree.put("hello", "world");
        tree.put("a", "b");
        tree.put("c", "d");
        tree.put("e", "f");
        tree.put("bob", "fred");
        tree.put("cat", "dog");
        tree.put("b", "x");
        tree.put("d", "y");

        System.out.println("\n\n\n");
        System.out.println(tree.toString());
    }
}
