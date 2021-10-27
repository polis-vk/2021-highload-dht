package ru.mail.polis.service.alex;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;

import java.util.Set;

public class Topology {

    private final String mePath;
    private final Node[] nodes;

    @SuppressWarnings("UnstableApiUsage")
    private final HashFunction hashFunction = Hashing.murmur3_128();

    public Topology(String mePath, Set<String> nodePaths) {
        this.mePath = mePath;
        this.nodes = new Node[nodePaths.size()];
        initNodes(nodePaths);
    }

    private void initNodes(final Set<String> topology) {
        int i = 0;
        for (String nodePath : topology) {
            HttpClient httpClient = new HttpClient(new ConnectionString(nodePath + "?timeout=500"));
            nodes[i] = new Node(nodePath, httpClient);
            i++;
        }
    }

    public boolean isMe(Node node) {
        return node.getPath().equals(mePath);
    }

    @SuppressWarnings("UnstableApiUsage")
    public Node getNode(String entityId) {
        int index = Hashing.consistentHash(hashFunction.hashBytes(entityId.getBytes()), nodes.length);
        return nodes[index];
    }

    public void stop() {
        for (Node node : nodes) {
            node.getHttpClient().close();
        }
    }
}
