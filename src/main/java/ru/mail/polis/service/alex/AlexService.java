package ru.mail.polis.service.alex;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;
import java.io.IOException;

public class AlexService implements Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlexService.class);
    private final HttpServerConfig httpServerConfig;
    private final DAO dao;
    private HttpServer alexServer;

    public AlexService(final int port, final DAO dao) {
        this.httpServerConfig = createHttpServerConfig(port);
        this.dao = dao;
    }

    public static HttpServerConfig createHttpServerConfig(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[] { acceptorConfig };
        return httpServerConfig;
    }

    @Override
    public void start() {
        try {
            alexServer = new AlexServer(httpServerConfig, dao);
            alexServer.start();
        } catch (IOException e) {
            LOGGER.error("Server can not start!");
        }
    }

    @Override
    public void stop() {
        if (alexServer != null) {
            alexServer.stop();
        }
    }
}
