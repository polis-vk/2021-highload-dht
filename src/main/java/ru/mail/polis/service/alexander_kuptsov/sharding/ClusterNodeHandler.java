package ru.mail.polis.service.alexander_kuptsov.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.Fnv1Hash32;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClusterNodeHandler {
    private final ConsistentHashing consistentHashing;
    private final Map<String, HttpClient> servers;
    private String selfNode;

    public ClusterNodeHandler(Set<String> topology, final int selfPort) {
        this.consistentHashing = ConsistentHashing.createByTopology(topology, new Fnv1Hash32());
        this.servers = new HashMap<>(topology.size());
        createByTopology(topology, selfPort);
    }

    private void createByTopology(Set<String> topology, final int selfPort) {
        final String selfPortString = String.valueOf(selfPort);
        for (final String node : topology) {
            if (node.contains(selfPortString)) {
                this.selfNode = node;
            } else {
                var serverAccess = new HttpClient(new ConnectionString(node));
                this.servers.put(node, serverAccess);
            }
        }
    }

    public HttpClient getServer(String id) {
        final String node = consistentHashing.getServer(id);
        return servers.get(node);
    }

    public boolean isSelfNode(String id) {
        final String node = consistentHashing.getServer(id);
        return node.equals(selfNode);
    }
}
