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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for a sharded two node {@link Service} cluster.
 *
 * @author Vadim Tsesko
 */
class ShardingTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    @Override
    int getClusterSize() {
        return 2;
    }

    @Test
    void insert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = "key";
            final byte[] value = randomValue();

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert
                assertEquals(201, upsert(node, key, value).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    checkResponse(200, value, get(i, key));
                }
            }
        });
    }

    @Test
    void insertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = new byte[0];

            for (int node = 0; node < getClusterSize(); node++) {
                // Insert
                assertEquals(201, upsert(node, key, value).getStatus());

                // Check
                for (int i = 0; i < getClusterSize(); i++) {
                    checkResponse(200, value, get(i, key));
                }
            }
        });
    }

    @Test
    void
    lifecycle2keys() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key1 = randomId();
            final byte[] value1 = randomValue();
            final String key2 = randomId();
            final byte[] value2 = randomValue();

            // Insert 1
            assertEquals(201, upsert(0, key1, value1).getStatus());

            // Check
            assertArrayEquals(value1, get(0, key1).getBody());
            assertArrayEquals(value1, get(1, key1).getBody());

            // Insert 2
            assertEquals(201, upsert(1, key2, value2).getStatus());

            // Check
            assertArrayEquals(value1, get(0, key1).getBody());
            assertArrayEquals(value2, get(0, key2).getBody());
            assertArrayEquals(value1, get(1, key1).getBody());
            assertArrayEquals(value2, get(1, key2).getBody());

            // Delete 1
            assertEquals(202, delete(0, key1).getStatus());
            assertEquals(202, delete(1, key1).getStatus());

            // Check
            assertEquals(404, get(0, key1).getStatus());
            assertArrayEquals(value2, get(0, key2).getBody());
            assertEquals(404, get(1, key1).getStatus());
            assertArrayEquals(value2, get(1, key2).getBody());

            // Delete 2
            assertEquals(202, delete(0, key2).getStatus());
            assertEquals(202, delete(1, key2).getStatus());

            // Check
            assertEquals(404, get(0, key2).getStatus());
            assertEquals(404, get(1, key2).getStatus());
        });
    }

    @Test
    void upsert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value1 = randomValue();
            final byte[] value2 = randomValue();

            // Insert value1
            assertEquals(201, upsert(0, key, value1).getStatus());

            // Insert value2
            assertEquals(201, upsert(1, key, value2).getStatus());

            // Check value 2
            for (int i = 0; i < getClusterSize(); i++) {
                checkResponse(200, value2, get(i, key));
            }
        });
    }

    @Test
    void upsertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();
            final byte[] empty = new byte[0];

            // Insert value
            assertEquals(201, upsert(0, key, value).getStatus());

            // Insert empty
            assertEquals(201, upsert(0, key, empty).getStatus());

            // Check empty
            for (int i = 0; i < getClusterSize(); i++) {
                checkResponse(200, empty, get(i, key));
            }
        });
    }

    @Test
    void delete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value).getStatus());
            assertEquals(201, upsert(1, key, value).getStatus());

            // Delete
            assertEquals(202, delete(0, key).getStatus());

            // Check
            assertEquals(404, get(0, key).getStatus());
            assertEquals(404, get(1, key).getStatus());
        });
    }

    @Test
    void distribute() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 1).getStatus());
            assertEquals(201, upsert(1, key, value, 1, 1).getStatus());

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

                // Stop node 0
                stop(node);
            }
            assertEquals(1, copies);
        });
    }
}
