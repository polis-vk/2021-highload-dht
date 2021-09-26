package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

final class MyConfig extends HttpServerConfig {
    MyConfig(int port) {
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        this.acceptors = new AcceptorConfig[]{acceptorConfig};
    }
}