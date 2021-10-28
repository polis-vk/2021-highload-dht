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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for single node {@link ServiceServer} API.
 *
 * @author Vadim Tsesko
 */
class SingleNodeTest extends TestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static DAOConfig daoConfig;
    private static DAO dao;
    private static int port;
    private static String endpoint;
    private static ServiceServer storage;
    private static HttpClient client;

    @BeforeAll
    static void beforeAll() throws Exception {
        port = randomPort();
        daoConfig = new DAOConfig(FileUtils.createTempDirectory());
        dao = DAOFactory.create(daoConfig);
        endpoint = endpoint(port);
        storage = ServiceFactory.create(port, dao, Collections.singleton(endpoint));
        storage.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        reset();
    }

    @AfterAll
    static void afterAll() throws IOException {
        client.close();
        storage.stop();
        dao.close();
        FileUtils.recursiveDelete(daoConfig.dir);
    }

    private static void reset() {
        if (client != null) {
            client.close();
        }
        client = new HttpClient(
                new ConnectionString(
                        "http://localhost:" + port +
                                "?timeout=" + (TIMEOUT.toMillis() / 2)));
    }

    private String path(final String id) {
        return "/v0/entity?id=" + id;
    }

    private Response get(final String key) throws Exception {
        return client.get(path(key));
    }

    private Response delete(final String key) throws Exception {
        return client.delete(path(key));
    }

    private Response upsert(
            final String key,
            final byte[] data) throws Exception {
        return client.put(path(key), data);
    }

    @Test
    void emptyKey() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get("").getStatus());
            assertEquals(400, delete("").getStatus());
            assertEquals(400, upsert("", new byte[]{0}).getStatus());
        });
    }

    @Test
    void absentParameterRequest() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                400,
                client.get("/v0/entity").getStatus()));
    }

    @Test
    void badRequest() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                400,
                client.get("/abracadabra").getStatus()));
    }

    @Test
    void getAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                404,
                get("absent").getStatus()));
    }

    @Test
    void deleteAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                202,
                delete("absent").getStatus()));
    }

    @Test
    void insert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(key, value).getStatus());

            // Check
            final Response response = get(key);
            assertEquals(200, response.getStatus());
            assertArrayEquals(value, response.getBody());
        });
    }

    @Test
    void insertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = new byte[0];

            // Insert
            assertEquals(201, upsert(key, value).getStatus());

            // Check
            final Response response = get(key);
            assertEquals(200, response.getStatus());
            assertArrayEquals(value, response.getBody());
        });
    }

    @Test
    void lifecycle2keys() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key1 = randomId();
            final byte[] value1 = randomValue();
            final String key2 = randomId();
            final byte[] value2 = randomValue();

            // Insert 1
            assertEquals(201, upsert(key1, value1).getStatus());

            // Check
            assertArrayEquals(value1, get(key1).getBody());

            // Insert 2
            assertEquals(201, upsert(key2, value2).getStatus());

            // Check
            assertArrayEquals(value1, get(key1).getBody());
            assertArrayEquals(value2, get(key2).getBody());

            // Delete 1
            assertEquals(202, delete(key1).getStatus());

            // Check
            assertEquals(404, get(key1).getStatus());
            assertArrayEquals(value2, get(key2).getBody());

            // Delete 2
            assertEquals(202, delete(key2).getStatus());

            // Check
            assertEquals(404, get(key2).getStatus());
        });
    }

    @Test
    void upsert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value1 = randomValue();
            final byte[] value2 = randomValue();

            // Insert value1
            assertEquals(201, upsert(key, value1).getStatus());

            // Insert value2
            assertEquals(201, upsert(key, value2).getStatus());

            // Check value 2
            final Response response = get(key);
            assertEquals(200, response.getStatus());
            assertArrayEquals(value2, response.getBody());
        });
    }

    @Test
    void respectFileFolder() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert value
            assertEquals(201, upsert(key, value).getStatus());

            // Check value
            final Response response = get(key);
            assertEquals(200, response.getStatus());
            assertArrayEquals(value, response.getBody());

            // Remove data and recreate
            storage.stop();
            dao.close();
            FileUtils.recursiveDelete(daoConfig.dir);
            java.nio.file.Files.createDirectory(daoConfig.dir);
            dao = DAOFactory.create(daoConfig);
            port = randomPort();
            endpoint = endpoint(port);
            storage = ServiceFactory.create(port, dao, Collections.singleton(endpoint));
            storage.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            reset();

            // Check absent data
            assertEquals(404, get(key).getStatus());
        });
    }

    @Test
    void upsertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();
            final byte[] empty = new byte[0];

            // Insert value
            assertEquals(201, upsert(key, value).getStatus());

            // Insert empty
            assertEquals(201, upsert(key, empty).getStatus());

            // Check empty
            final Response response = get(key);
            assertEquals(200, response.getStatus());
            assertArrayEquals(empty, response.getBody());
        });
    }

    @Test
    void delete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(key, value).getStatus());

            // Delete
            assertEquals(202, delete(key).getStatus());

            // Check
            assertEquals(404, get(key).getStatus());
        });
    }
}
