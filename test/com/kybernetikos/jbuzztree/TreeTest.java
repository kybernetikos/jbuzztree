package com.kybernetikos.jbuzztree;

import com.kybernetikos.jbuzztree.apis.Storage;
import com.kybernetikos.jbuzztree.apis.SerializingStorage;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class TreeTest {
    @Test
    void put() {
        Storage<String, String, Integer> storage = new SerializingStorage<>();
        Tree<String, String, Integer> storedTree = new Tree<>(null, Comparator.naturalOrder(), storage, 3);

        storedTree.put("hello", "world");
        storedTree.put("boom", "boom");
        storedTree.put("shake", "room");
        storedTree.put("hello", "everyone");
        storedTree.put("eye", "sight");
        storedTree.put("nose", "smell");

        Tree<String, String, Integer> tree = new Tree<>(storedTree.getRef(), Comparator.naturalOrder(), storage, 3);

        assertEquals("world", tree.get("hello", null));
        assertEquals(null, tree.get("jim", null));
        assertEquals("room", tree.get("shake", null));
        assertEquals("smell", tree.get("nose", null));
        assertEquals("not-here", tree.get("fred", "not-here"));
    }

}