package ru.mail.polis.service.sachuk.ilya.sharding;

import one.nio.net.ConnectionString;
import one.nio.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.sachuk.ilya.Pair;

import java.io.Closeable;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NodeManager implements Closeable {
    private Logger logger = LoggerFactory.getLogger(NodeManager.class);
    private final NavigableMap<Integer, VNode> circle = new TreeMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final NavigableMap<String, HttpClient> clients;
    private final ExecutorService coordinatorExecutor = new ThreadPoolExecutor(8, 8,
            0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2000));
    private final VNodeConfig vnodeConfig;

    public NodeManager(Set<String> topology, VNodeConfig vnodeConfig, Node node) {
        this.vnodeConfig = vnodeConfig;

        clients = new TreeMap<>();

        for (String endpoint : topology) {
            ConnectionString connectionString = new ConnectionString(endpoint);
            logger.info(connectionString.toString());

            addNode(new Node(connectionString.getPort()));
            if (node.port == connectionString.getPort()) {
                continue;
            }

            HttpClient client = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(3))
                    .executor(coordinatorExecutor)
                    .build();
            clients.put(endpoint, client);
        }
    }

    public Pair<Integer, VNode> getNearVNodeWithGreaterHash(String key, Integer hash, List<Integer> currentPorts) {
        checkIsClosed();

        Map.Entry<Integer, VNode> integerVNodeEntry;
        integerVNodeEntry = circle.higherEntry(Objects.requireNonNullElseGet(hash, () -> Hash.murmur3(key)));

        VNode vnode;
        Integer hashReturn;
        if (integerVNodeEntry == null) {
            vnode = circle.firstEntry().getValue();
            hashReturn = circle.firstEntry().getKey();
        } else {
            vnode = integerVNodeEntry.getValue();
            hashReturn = integerVNodeEntry.getKey();
        }

        if (currentPorts.contains(vnode.getPhysicalNode().port)) {
            return getNearVNodeWithGreaterHash(key, hashReturn, currentPorts);
        }

        return new Pair<>(hashReturn, vnode);
    }

    public HttpClient getHttpClient(String endpoint) {
        return clients.get(endpoint);
    }

    @Override
    public void close() {
        isClosed.set(true);

    }

    private void addNode(Node node) {
        checkIsClosed();

        for (int i = 0; i < vnodeConfig.nodeWeight; i++) {
            int hashCode = Hash.murmur3(Node.HOST + node.port + i);

            circle.put(hashCode, new VNode(node));
        }
    }

    private void checkIsClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("NodeManager is already closed");
        }
    }
}
