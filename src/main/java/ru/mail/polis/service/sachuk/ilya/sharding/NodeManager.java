package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.util.Hash;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class NodeManager {
    private static volatile NodeManager instance;
    private final Set<String> topology;
    private VNodeConfig vNodeConfig;
    private final NavigableMap<Integer, VNode> circle;

    private NodeManager(Set<String> topology, VNodeConfig vNodeConfig) {
        this.topology = topology;
        this.vNodeConfig = vNodeConfig;
        circle = new TreeMap<>();
    }

    public static NodeManager getInstance(Set<String> topology, VNodeConfig vNodeConfig) {
        if (instance == null) {
            synchronized (NodeManager.class) {
                if (instance == null) {

                    instance = new NodeManager(topology, vNodeConfig);
                    return instance;
                }
            }
        }

        return instance;
    }

    public void addNode(Node node) {
        for (int i = 0; i < vNodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(node.host + node.port);

            circle.put(hashCode, new VNode(node));
        }
    }

    public VNode getNearVNode(String key) {
        Map.Entry<Integer, VNode> integerVNodeEntry = circle.ceilingEntry(Hash.murmur3(key));

        return integerVNodeEntry.getValue();
    }
}
