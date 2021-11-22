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

package ru.mail.polis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.DAOFactory;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.ServiceFactory;
import ru.mail.polis.sharding.ConsistentHashRouter;
import ru.mail.polis.sharding.HashFunction;
import ru.mail.polis.sharding.HashRouter;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Starts 3-node storage cluster and waits for shutdown.
 *
 * @author Vadim Tsesko
 */
public final class Cluster {
    private static final int[] PORTS = {8080, 8081, 8082};

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    private Cluster() {
        // Not instantiable
    }

    public static void main(String[] args) throws IOException {
        // Fill the topology
        final Set<String> topology = new HashSet<>(3);
        for (final int port : PORTS) {
            topology.add("http://localhost:" + port);
        }

        // Start nodes
        for (int i = 0; i < PORTS.length; i++) {
            final int port = PORTS[i];
            final DAOConfig config = new DAOConfig(FileUtils.createTempDirectory());
            final DAO dao = DAOFactory.create(config);

            LOG.info("Starting node {} on port {} and data at {}", i, port, config.dir);

            // Start the storage
            final Service storage =
                    ServiceFactory.create(
                            port,
                            dao,
                            topology);
            storage.start();
            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        storage.stop();
                        try {
                            dao.close();
                        } catch (IOException e) {
                            LOG.error("Can't close dao", e);
                        }
                    }));
        }
    }

    public static class Node implements ru.mail.polis.sharding.Node {
        public final String ip;
        public final int port;
        private final HttpClient httpClient;

        public Node(String host, int port, HttpClient httpClient) {
            this.ip =  host;
            this.port = port;
            this.httpClient = httpClient;
        }

        @Override
        public String getKey() {
            return ip + ":" + port;
        }

        public Node init() {
            //httpClient = HttpUtils.createClient(Executors.newFixedThreadPool(4)); // FIXME
            return this;
        }

        public void close() {
            // FIXME: remove or ???
        }

        public HttpClient getClient() {
            return httpClient;
        }
    }

    public static class ReplicasHolder {
        public final int replicasCount;
        private final Map<Node, List<Node>> replicas;

        public ReplicasHolder(int maxReplicas, Set<Node> topology, Comparator<Node> comparator) {
            if (maxReplicas < 1) {
                throw new IllegalArgumentException("max. replicas < 1");
            } else if (maxReplicas > topology.size()) {
                throw new IllegalArgumentException("max. replicas > nodes in topology (=" + topology.size() + ")");
            }

            replicasCount = maxReplicas;
            replicas = new HashMap<>(replicasCount);

            HashRouter<Node> router = new ConsistentHashRouter<>(topology, 30, new HashFunction.HashXXH3());
            for (Node node : topology) {
                replicas.computeIfAbsent(node, key -> computeReplicas(node, router, comparator));

                StringJoiner joiner = new StringJoiner(", ");
                replicas.get(node).forEach(n -> {
                    if (n != node) {
                        joiner.add(n.getKey());
                    }
                });
                LOG.info("Created replicas for node {}: {}", node.getKey(), joiner);
            }
        }

        public List<Node> getBunch(Node target) {
            return replicas.get(target);
        }

        public List<Node> getBunch(Node target, int max) {
            return getBunch(target).subList(0, max);
        }

        private List<Node> computeReplicas(Node node, HashRouter<Node> router, Comparator<Node> comparator) {
            Set<Node> replicas = new HashSet<>();
            for (int i = 0; replicas.size() < replicasCount; i++) {
                Node next = router.route(node.getKey() + ":" + i);
                replicas.add(next);
            }

            List<Node> replicasList = new ArrayList<>(replicas.size());
            replicasList.addAll(replicas);
            replicasList.sort(comparator);
            return Collections.unmodifiableList(replicasList);
        }
    }
}
