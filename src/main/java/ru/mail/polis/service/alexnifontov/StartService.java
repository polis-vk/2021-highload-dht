package ru.mail.polis.service.alexnifontov;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;

public class StartService implements Service {
    private final DAO dao;
    private final HttpServerConfig config;
    private HttpServer server;

    public StartService(final int port, final DAO dao) {
        this.dao = dao;

        this.config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        this.config.acceptors = new AcceptorConfig[]{acceptor};
    }

    @Override
    public void start() {
        try {
            this.server = new BasicService(config, dao);
            this.server.start();
        } catch (IOException e) {
            throw new RuntimeException("The server cannot be started", e);
        }

    }

    @Override
    public void stop() {
        if (this.server != null) {
            this.server.stop();
        }
    }
}
