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

package ru.mail.polis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.DAOFactory;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.ServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Starts storage and waits for shutdown.
 *
 * @author Vadim Tsesko
 */
public final class Server {
    private static final int PORT = 8080;

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private Server() {
        // Not instantiable
    }

    private static void startDao(Path data) {
        // Start the storage
        try {
            DAO dao = DAOFactory.create(new DAOConfig(data));
            final Service storage =
                    ServiceFactory.create(
                            PORT,
                            dao);
            storage.start();
            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        storage.stop();
                        try {
                            dao.close();
                        } catch (IOException e) {
                            throw new RuntimeException("Can't close dao", e);
                        }
                    }));
        } catch (IOException e) {
            LOG.error("IO caught in dao initializing", e);
        }
    }

    /**
     * Starts single node cluster at HTTP port 8080 and
     * temporary data storage if storage path not supplied.
     */
    public static void main(String[] args) throws IOException {
        // Temporary storage in the file system
        final Path data;
        if (args.length > 0) {
            data = Path.of(args[0]);
        } else {
            data = FileUtils.createTempDirectory();
        }
        LOG.info("Storing data at {}", data);
        startDao(data);

    }
}
