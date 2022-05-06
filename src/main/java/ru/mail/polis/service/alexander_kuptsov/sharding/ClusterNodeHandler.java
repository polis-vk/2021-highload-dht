package ru.mail.polis.service.alexander_kuptsov.sharding;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClusterNodeHandler {
    private final IDistributionAlgorithm distributionAlgorithm;
    private final Map<String, HttpClient> servers;
    private String selfNode;

    public ClusterNodeHandler(Set<String> topology, int selfPort, IDistributionAlgorithm distributionAlgorithm) {
        this.distributionAlgorithm = distributionAlgorithm;
        this.servers = new HashMap<>(topology.size());
        init(topology, selfPort);
    }

    private void init(Set<String> topology, int selfPort) {
        addTopology(topology);
        createByTopology(topology, selfPort);
    }

    private void createByTopology(Set<String> topology, int selfPort) {
        String selfPortString = String.valueOf(selfPort);
        for (String node : topology) {
            if (node.contains(selfPortString)) {
                this.selfNode = node;
            } else {
                HttpClient serverAccess = new HttpClient(new ConnectionString(node));
                this.servers.put(node, serverAccess);
            }
        }
    }

    public HttpClient getServer(String id) {
        String node = distributionAlgorithm.getServer(id);
        return servers.get(node);
    }

    public boolean isSelfNode(String id) {
        String node = distributionAlgorithm.getServer(id);
        return node.equals(selfNode);
    }

    public void addTopology(Set<String> topology) {
        distributionAlgorithm.addTopology(topology);
    }

    public void addServer(String server) {
        distributionAlgorithm.addServer(server);
    }

    public void removeServer(String server) {
        distributionAlgorithm.removeServer(server);
    }

    public void removeTopology(Set<String> topology) {
        distributionAlgorithm.removeTopology(topology);
    }
}
