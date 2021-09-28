package ru.mail.polis.service.kuzoliza;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;

public class StartService implements Service {

    private final HttpServerConfig config;
    private final DAO dao;
    private HttpServer server;
    private static final Logger LOG = LoggerFactory.getLogger(StartService.class.getName());

    /**
     * Service configuration.
     *
     * @param port - which port should be listened
     * @param dao - database
     */
    public StartService(final int port, final DAO dao) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        this.dao = dao;
        this.config = new HttpServerConfig();
        this.config.acceptors = new AcceptorConfig[]{acceptorConfig};
    }

    @Override
    public void start() {
        try {
            this.server = new MyService(config, dao);
            this.server.start();
        } catch (IOException e) {
            LOG.error("Can't start server");
        }
    }

    @Override
    public void stop() {
        if (this.server != null) {
            this.server.stop();
        }
    }
}
