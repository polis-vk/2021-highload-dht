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

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import ru.mail.polis.FileUtils;
import ru.mail.polis.TestBase;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.DAOFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for single node range API.
 *
 * @author Vadim Tsesko
 */
@Disabled 
class SingleRangeTest extends TestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static DAOConfig daoConfig;
    private static DAO dao;
    private static int port;
    private static Service storage;
    private static HttpClient client;

    private static void reset() {
        if (client != null) {
            client.close();
        }
        client = new HttpClient(
                new ConnectionString(
                        "http://localhost:" + port +
                                "?timeout=" + (TIMEOUT.toMillis() / 2)));
    }

    private static byte[] chunkOf(
            final String key,
            final String value) {
        return (key + '\n' + value).getBytes();
    }

    @BeforeEach
    void beforeAll() throws Exception {
        port = randomPort();
        daoConfig = new DAOConfig(FileUtils.createTempDirectory());
        dao = DAOFactory.create(daoConfig);
        storage = ServiceFactory.create(port, dao, Collections.singleton(endpoint(port)));
        storage.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        reset();
    }

    @AfterEach
    void afterAll() throws IOException {
        client.close();
        storage.stop();
        dao.close();
        FileUtils.recursiveDelete(daoConfig.dir);
    }

    private Response range(
            final String start,
            final String end) throws Exception {
        return client.get("/v0/entities?start=" + start + (end != null ? "&end=" + end : ""));
    }

    private Response upsert(
            final String key,
            final byte[] data) throws Exception {
        return client.put("/v0/entity?id=" + key, data);
    }

    @Test
    void emptyKey() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, range("", "").getStatus());
            assertEquals(400, upsert("", new byte[]{0}).getStatus());
        });
    }

    @Test
    void absentParameterRequest() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, client.get("/v0/entities").getStatus());
            assertEquals(400, client.get("/v0/entities?end=end").getStatus());
        });
    }

    @Test
    void getAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range("absent0", "absent1");
            assertEquals(200, response.getStatus());
            assertEquals(0, response.getBody().length);
        });
    }

    @Test
    void single() {
        final String prefix = "single";
        final String key = prefix + 1;
        final String value = "value1";

        assertTimeoutPreemptively(TIMEOUT, () -> {
            // Insert
            assertEquals(201, upsert(key, value.getBytes()).getStatus());

            // Check
            final Response response = range(key, prefix + 2);
            assertEquals(200, response.getStatus());
            assertArrayEquals(chunkOf(key, value), response.getBody());
        });

        // Excluding the key
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range("a", key);
            assertEquals(200, response.getStatus());
            assertEquals(0, response.getBody().length);
        });

        // After the key
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 2, prefix + 3);
            assertEquals(200, response.getStatus());
            assertEquals(0, response.getBody().length);
        });
    }

    @Test
    void triple() {
        final String prefix = "triple";
        final String value1 = "value1";
        final String value2 = "";
        final String value3 = "value3";

        // Insert reversed
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(201, upsert(prefix + 3, value3.getBytes()).getStatus());
            assertEquals(201, upsert(prefix + 2, value2.getBytes()).getStatus());
            assertEquals(201, upsert(prefix + 1, value1.getBytes()).getStatus());
        });

        // Check all
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final byte[] chunk1 = chunkOf(prefix + 1, value1);
            final byte[] chunk2 = chunkOf(prefix + 2, value2);
            final byte[] chunk3 = chunkOf(prefix + 3, value3);
            final byte[] expected = new byte[chunk1.length + chunk2.length + chunk3.length];
            System.arraycopy(chunk1, 0, expected, 0, chunk1.length);
            System.arraycopy(chunk2, 0, expected, chunk1.length, chunk2.length);
            System.arraycopy(chunk3, 0, expected, expected.length - chunk3.length, chunk3.length);

            final Response response = range(prefix + 1, prefix + 4);
            assertEquals(200, response.getStatus());
            assertArrayEquals(expected, response.getBody());
        });

        // To the left
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 0, prefix + 1);
            assertEquals(200, response.getStatus());
            assertEquals(0, response.getBody().length);
        });

        // First left
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 0, prefix + 2);
            assertEquals(200, response.getStatus());
            assertArrayEquals(chunkOf(prefix + 1, value1), response.getBody());
        });

        // First point
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 1, prefix + 2);
            assertEquals(200, response.getStatus());
            assertArrayEquals(chunkOf(prefix + 1, value1), response.getBody());
        });

        // Second point
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 2, prefix + 3);
            assertEquals(200, response.getStatus());
            assertArrayEquals(chunkOf(prefix + 2, value2), response.getBody());
        });

        // Third point
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 3, prefix + 4);
            assertEquals(200, response.getStatus());
            assertArrayEquals(chunkOf(prefix + 3, value3), response.getBody());
        });

        // To the right
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final Response response = range(prefix + 4, null);
            assertEquals(200, response.getStatus());
            assertEquals(0, response.getBody().length);
        });
    }
}
