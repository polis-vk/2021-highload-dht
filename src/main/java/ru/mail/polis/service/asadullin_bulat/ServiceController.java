package ru.mail.polis.service.asadullin_bulat;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

public final class ServiceController {

    private ServiceController() {
        throw new IllegalStateException("Utility class");
    }

    public static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};

        return config;
    }

}
