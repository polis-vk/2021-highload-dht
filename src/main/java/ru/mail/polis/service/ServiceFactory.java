/*
 * Copyright 2020 (c) Odnoklassniki
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

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.gasparyansokrat.ServiceConfig;
import ru.mail.polis.service.gasparyansokrat.ServiceImpl;
import ru.mail.polis.service.gasparyansokrat.ThreadPoolConfig;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Constructs {@link Service} instances.
 *
 * @author Vadim Tsesko
 */
public final class ServiceFactory {
    private static final long MAX_HEAP = 128 * 1024 * 1024;
    private static final int POOL_SIZE = 4;
    private static final int QUEUE_SIZE = 128;

    private ServiceFactory() {
        // Not supposed to be instantiated
    }

    /**
     * Construct a storage instance.
     *
     * @param port port to bind HTTP server to
     * @param dao  DAO to store the data
     * @return a storage instance
     */
    public static Service create(
            final int port,
            final DAO dao) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 1 << 16 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        Objects.requireNonNull(dao);

        ServiceConfig servConfig = new ServiceConfig(port, ThreadPoolConfig.MAX_THREADS, "localhost");
        ThreadPoolConfig threadPoolConfig = new ThreadPoolConfig(POOL_SIZE, QUEUE_SIZE, 5, TimeUnit.SECONDS);
        return new ServiceImpl(servConfig, threadPoolConfig, dao);
    }
}
