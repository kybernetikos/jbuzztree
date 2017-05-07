package com.kybernetikos.jbuzztree;

import com.kybernetikos.jbuzztree.apis.Api;
import com.kybernetikos.jbuzztree.apis.MemoryApi;
import com.kybernetikos.jbuzztree.apis.SerializingApi;

import java.util.Comparator;

public class Main {

    public static void main(String[] args) {
        Api<String, String, Integer> api = new SerializingApi<>();
        Tree<String, String, Integer> storedTree = new Tree<>(null, Comparator.naturalOrder(), api, 3);

        storedTree.put("hello", "world");
        storedTree.put("boom", "boom");
        storedTree.put("shake", "room");
        storedTree.put("hello", "everyone");
        storedTree.put("eye", "sight");
        storedTree.put("nose", "smell");

        Tree<String, String, Integer> tree = new Tree<>(storedTree.getRef(), Comparator.naturalOrder(), api, 3);

        System.out.println(tree.get("hello", null));
        System.out.println(tree.get("jim", null));
        System.out.println(tree.get("shake", null));
        System.out.println(tree.get("nose", null));
        System.out.println(tree.get("fred", "not-here"));
    }
}
