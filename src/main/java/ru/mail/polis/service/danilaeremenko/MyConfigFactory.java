package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

final class MyConfigFactory {
    private MyConfigFactory() {
    }

    public static HttpServerConfig fromPort(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        HttpServerConfig resConfig = new HttpServerConfig();
        resConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return resConfig;
    }

    public static HttpServerConfig fromPortWorkers(int port, int maxWorkers) {
        HttpServerConfig resConfig = fromPort(port);
        resConfig.maxWorkers = maxWorkers;
        return resConfig;
    }
}
