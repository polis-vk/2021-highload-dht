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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for a two node replicated {@link Service} cluster.
 *
 * @author Vadim Tsesko
 */
class TwoNodeTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    @Override
    int getClusterSize() {
        return 2;
    }

    @Test
    void tooSmallRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 0, 2).getStatus());
            assertEquals(400, upsert(0, randomId(), randomValue(), 0, 2).getStatus());
            assertEquals(400, delete(0, randomId(), 0, 2).getStatus());
        });
    }

    @Test
    void tooBigRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 3, 2).getStatus());
            assertEquals(400, upsert(0, randomId(), randomValue(), 3, 2).getStatus());
            assertEquals(400, delete(0, randomId(), 3, 2).getStatus());
        });
    }

    @Test
    void unreachableRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            stop(0);
            assertEquals(504, get(1, randomId(), 2, 2).getStatus());
            assertEquals(504, upsert(1, randomId(), randomValue(), 2, 2).getStatus());
            assertEquals(504, delete(1, randomId(), 2, 2).getStatus());
        });
    }

    @Test
    void overlapRead() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            // Insert value1
            final byte[] value1 = randomValue();
            assertEquals(201, upsert(0, key, value1, 1, 2).getStatus());

            // Check
            checkResponse(200, value1, get(0, key, 2, 2));
            checkResponse(200, value1, get(1, key, 2, 2));

            // Help implementors with ms precision for conflict resolution
            waitForVersionAdvancement();

            // Insert value2
            final byte[] value2 = randomValue();
            assertEquals(201, upsert(1, key, value2, 1, 2).getStatus());

            // Check
            checkResponse(200, value2, get(0, key, 2, 2));
            checkResponse(200, value2, get(1, key, 2, 2));
        });
    }

    @Test
    void overlapWrite() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            // Insert value1
            final byte[] value1 = randomValue();
            assertEquals(201, upsert(0, key, value1, 2, 2).getStatus());

            // Check
            checkResponse(200, value1, get(0, key, 1, 2));
            checkResponse(200, value1, get(1, key, 1, 2));

            // Help implementors with ms precision for conflict resolution
            waitForVersionAdvancement();

            // Insert value2
            final byte[] value2 = randomValue();
            assertEquals(201, upsert(1, key, value2, 2, 2).getStatus());

            // Check
            checkResponse(200, value2, get(0, key, 1, 2));
            checkResponse(200, value2, get(1, key, 1, 2));
        });
    }

    @Test
    void overlapDelete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert & delete at 0
            assertEquals(201, upsert(0, key, value, 2, 2).getStatus());
            waitForVersionAdvancement();
            assertEquals(202, delete(0, key, 2, 2).getStatus());

            // Check
            assertEquals(404, get(0, key, 1, 2).getStatus());
            assertEquals(404, get(1, key, 1, 2).getStatus());

            // Insert & delete at 1
            assertEquals(201, upsert(1, key, value, 2, 2).getStatus());
            waitForVersionAdvancement();
            assertEquals(202, delete(1, key, 2, 2).getStatus());

            // Check
            assertEquals(404, get(0, key, 1, 2).getStatus());
            assertEquals(404, get(1, key, 1, 2).getStatus());
        });
    }

    @Test
    void missedWrite() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize
                restartAllNodes();

                // Stop node
                stop(node);

                // Insert
                final byte[] value = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value, 1, 2).getStatus());

                // Start node 1
                createAndStart(node);

                // Check
                checkResponse(200, value, get(node, key, 2, 2));

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
                final byte[] value = randomValue();
                assertEquals(201, upsert(node, key, value, 2, 2).getStatus());

                // Stop node 0
                stop(node);

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Delete
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 1, 2).getStatus());

                // Start node 0
                createAndStart(node);

                // Check
                assertEquals(404, get(node, key, 2, 2).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void missedOverwrite() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();

            for (int node = 0; node < getClusterSize(); node++) {
                // Reinitialize
                restartAllNodes();

                // Insert value1
                final byte[] value1 = randomValue();
                assertEquals(201, upsert(node, key, value1, 2, 2).getStatus());

                // Stop node
                stop(node);

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Insert value2
                final byte[] value2 = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value2, 1, 2).getStatus());

                // Start node
                createAndStart(node);

                // Check value2
                checkResponse(200, value2, get(node, key, 2, 2));

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
                assertEquals(201, upsert(node, key, value1, 2, 2).getStatus());
                waitForVersionAdvancement();
                assertEquals(202, delete(node, key, 2, 2).getStatus());

                // Stop node
                stop(node);

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Insert value2
                final byte[] value2 = randomValue();
                assertEquals(201, upsert((node + 1) % getClusterSize(), key, value2, 1, 2).getStatus());

                // Start node
                createAndStart(node);

                // Check value2
                checkResponse(200, value2, get(node, key, 2, 2));

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
                assertEquals(201, upsert(node, key, value, 2, 2).getStatus());

                // Stop node
                stop(node);

                // Check
                checkResponse(200, value, get((node + 1) % getClusterSize(), key, 1, 2));

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();

                // Delete
                assertEquals(202, delete((node + 1) % getClusterSize(), key, 1, 2).getStatus());

                // Check
                assertEquals(404, get((node + 1) % getClusterSize(), key, 1, 2).getStatus());

                // Help implementors with ms precision for conflict resolution
                waitForVersionAdvancement();
            }
        });
    }

    @Test
    void respectRF() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 1).getStatus());

            // Stop all
            for (int node = 0; node < getClusterSize(); node++) {
                stop(node);
            }

            // Check each
            int copies = 0;
            for (int node = 0; node < getClusterSize(); node++) {
                // Start node
                createAndStart(node);

                // Check
                if (get(node, key, 1, 1).getStatus() == 200) {
                    copies++;
                }

                // Stop node
                stop(node);
            }
            assertEquals(1, copies);
        });
    }

    @Test
    @Disabled("Not implemented functionality")
    void repairCheck() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Stopping 1
            stop(1);

            // Insert at 0
            assertEquals(201, upsert(0, key, value, 1, 2).getStatus());

            // Stopping 0
            stop(0);

            // Starting 1
            createAndStart(1);

            // Check 1
            assertEquals(404, get(1, key, 1, 2).getStatus());

            // Starting 0
            createAndStart(0);

            // Check 0 & 1
            assertEquals(200, get(0, key, 2, 2).getStatus());

            // Help implementors with ms precision for conflict resolution
            waitForVersionAdvancement();

            // Stopping 0
            stop(0);

            // Check repair between 0 & 1
            assertEquals(200, get(1, key, 1, 2).getStatus());
        });
    }
}
