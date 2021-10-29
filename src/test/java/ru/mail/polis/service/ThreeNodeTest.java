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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for a three node replicated {@link Service} cluster.
 *
 * @author Vadim Tsesko
 */
class ThreeNodeTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    @Override
    int getClusterSize() {
        return 3;
    }

    @Test
    void tooSmallRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 0, 3).getStatus());
            assertEquals(400, upsert(0, randomId(), randomValue(), 0, 3).getStatus());
            assertEquals(400, delete(0, randomId(), 0, 3).getStatus());
        });
    }

    @Test
    void tooBigRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 4, 3).getStatus());
            assertEquals(400, upsert(0, randomId(), randomValue(), 4, 3).getStatus());
            assertEquals(400, delete(0, randomId(), 4, 3).getStatus());
        });
    }

    @Test
    void unreachableRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            stop(0);
            assertEquals(504, get(1, randomId(), 3, 3).getStatus());
            assertEquals(504, upsert(1, randomId(), randomValue(), 3, 3).getStatus());
            assertEquals(504, delete(1, randomId(), 3, 3).getStatus());
        });
    }

    @Test
    void overlapQuorum() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert
                final byte[] value = randomValue();
                assertEquals(201, upsert(node, key, value, 2, 3).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    checkResponse(200, value, get(i, key, 2, 3));
                }
            }
        });
    }

    @Test
    void overlap() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert
                final byte[] value = randomValue();
                assertEquals(201, upsert(node, key, value, 1, 3).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    checkResponse(200, value, get(i, key, 3, 3));
                }
            }
        });
    }

    @Test
    void overlapQuorumDelete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert & delete
                assertEquals(201, upsert(node, key, randomValue(), 3, 3).getStatus());
                waitForVersionAdvancement();
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 2, 3).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    assertEquals(404, get(i, key, 2, 3).getStatus());
                }
            }
        });
    }

    @Test
    void overlapDelete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert & delete
                assertEquals(201, upsert(node, key, randomValue(), 3, 3).getStatus());
                waitForVersionAdvancement();
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 1, 3).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    assertEquals(404, get(i, key, 3, 3).getStatus());
                }
            }
        });
    }

    @Test
    void missedWrite() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize cluster
                restartAllNodes();

                // Stop node
                stop(node);

                // Insert value
                final byte[] value = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check
                checkResponse(200, value, get(node, key, 2, 3));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void missedRecreate() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize
                restartAllNodes();

                // Insert & delete value1
                final byte[] value1 = randomValue();
                assertEquals(201, upsert(node, key, value1, 3, 3).getStatus());
                waitForVersionAdvancement();
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 3, 3).getStatus());

                // Stop node
                stop(node);

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Insert value2
                final byte[] value2 = randomValue();
                assertEquals(201, upsert((node + 2) % getClusterSize(), key, value2, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check value2
                checkResponse(200, value2, get(node, key, 3, 3));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void missedDelete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize
                restartAllNodes();

                // Insert
                assertEquals(201, upsert(node, key, randomValue(), 3, 3).getStatus());

                // Stop node
                stop(node);

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Delete
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 2, 3).getStatus());

                // Start node
                createAndStart(node);

                // Check
                assertEquals(404, get(node, key, 3, 3).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void tolerateFailure() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize
                restartAllNodes();

                // Insert into node
                final byte[] value = randomValue();
                assertEquals(201, upsert(node, key, value, 3, 3).getStatus());

                // Stop node
                stop(node);

                // Check
                checkResponse(200, value, get((node + 1) % getClusterSize(), key, 2, 3));
                checkResponse(200, value, get((node + 2) % getClusterSize(), key, 2, 3));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Delete
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 2, 3).getStatus());

                // Check
                assertEquals(404, get((node + 1) % getClusterSize(), key, 2, 3).getStatus());
                assertEquals(404, get((node + 2) % getClusterSize(), key, 2, 3).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void respectRF1() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 1).getStatus());

            // Stop all nodes
            for (int i = 0; i < getClusterSize(); i++) {
                stop(i);
            }

            // Check each node
            int copies = 0;
            for (int i = 0; i < getClusterSize(); i++) {
                // Start node
                createAndStart(i);

                // Check node
                if (get(i, key, 1, 1).getStatus() == 200) {
                    copies++;
                }

                // Stop node
                stop(i);
            }
            assertEquals(1, copies);
        });
    }

    @Test
    void respectRF2() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 2).getStatus());

            // Stop all nodes
            for (int i = 0; i < getClusterSize(); i++) {
                stop(i);
            }

            // Check each node
            int copies = 0;
            for (int i = 0; i < getClusterSize(); i++) {
                // Start node
                createAndStart(i);

                // Check node
                if (get(i, key, 1, 2).getStatus() == 200) {
                    copies++;
                }

                // Stop node
                stop(i);
            }
            assertEquals(2, copies);
        });
    }
}
