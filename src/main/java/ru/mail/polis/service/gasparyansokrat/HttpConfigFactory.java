package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

final class HttpConfigFactory {

    private HttpConfigFactory() {

    }

    public static HttpServerConfig buildHttpConfig(final ServiceConfig servConfig) {
        AcceptorConfig accConf = new AcceptorConfig();
        accConf.port = servConfig.port;
        accConf.address = servConfig.address;
        accConf.reusePort = true;
        accConf.threads = servConfig.poolSize;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{accConf};
        return config;
    }
}
