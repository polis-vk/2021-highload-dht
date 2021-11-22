/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.service;

import ru.mail.polis.Cluster;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.eldar_tim.HttpServerImpl;
import ru.mail.polis.service.eldar_tim.LimitedServiceExecutor;
import ru.mail.polis.service.eldar_tim.ServiceExecutor;
import ru.mail.polis.sharding.ConsistentHashRouter;
import ru.mail.polis.sharding.HashRouter;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Constructs {@link Service} instances.
 */
public final class ServiceFactory {
    /** Максимальный размер кучи. */
    private static final long MAX_HEAP = 512 * 1024 * 1024;

    /** Число рабочих потоков. */
    private static final int WORKERS_NUMBER = Runtime.getRuntime().availableProcessors();
    /** Лимит очереди запросов, после превышения которого последующие будут отвергнуты. */
    private static final int TASKS_LIMIT = WORKERS_NUMBER * 2;

    /** Число репликаций для каждого узла, включая сам узел. Минимальное значение = 1. */
    private static final int REPLICAS_NUMBER = 3;

    private static final Map<Integer, Set<Cluster.Node>> TOPOLOGIES = new ConcurrentHashMap<>();
    private static final Map<Integer, Cluster.ReplicasHolder> REPLICAS = new ConcurrentHashMap<>();

    private ServiceFactory() {
        // Not supposed to be instantiated
    }

    /**
     * Construct a storage instance.
     *
     * @param port     port to bind HTTP server to
     * @param dao      DAO to store the data
     * @param topology a list of all cluster endpoints {@code http://<host>:<port>} (including this one)
     * @return a storage instance
     */
    public static Service create(
            final int port,
            final DAO dao,
            final Set<String> topology) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 1 << 16 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        Objects.requireNonNull(dao);

        if (topology.isEmpty()) {
            throw new IllegalArgumentException("Empty cluster");
        }

        Set<Cluster.Node> clusterNodes = TOPOLOGIES.computeIfAbsent(topology.hashCode(),
                key -> buildClusterNodes(topology));

        Comparator<Cluster.Node> comparator = Comparator.comparing(Cluster.Node::getKey);
        Cluster.ReplicasHolder replicasHolder = REPLICAS.computeIfAbsent(topology.hashCode(),
                key -> new Cluster.ReplicasHolder(Math.min(REPLICAS_NUMBER, topology.size()),
                        clusterNodes, comparator));

        Cluster.Node currentNode = findClusterNode(port, clusterNodes);
        HashRouter<Cluster.Node> hashRouter = new ConsistentHashRouter<>(clusterNodes, 30);

        ServiceExecutor workers = new LimitedServiceExecutor("worker", WORKERS_NUMBER, TASKS_LIMIT);
        ServiceExecutor ioWorkers = new LimitedServiceExecutor("net_io", WORKERS_NUMBER, TASKS_LIMIT);

        return new HttpServerImpl(dao, currentNode, replicasHolder, hashRouter, workers, ioWorkers);
    }

    private static Set<Cluster.Node> buildClusterNodes(Set<String> topologyRaw) {
        Set<Cluster.Node> topology = new HashSet<>(topologyRaw.size());
        for (String endpoint : topologyRaw) {
            Cluster.Node node = new Cluster.Node(endpoint);
            topology.add(node);
        }
        return topology;
    }

    private static Cluster.Node findClusterNode(int port, Collection<Cluster.Node> topology) {
        for (Cluster.Node node : topology) {
            if (node.port == port) {
                return node;
            }
        }
        throw new IllegalArgumentException("Port not presented in the collection");
    }
}
