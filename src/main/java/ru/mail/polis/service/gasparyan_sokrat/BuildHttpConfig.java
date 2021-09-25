package ru.mail.polis.service.gasparyan_sokrat;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

final class HttpConfigFactory {

    public static HttpServerConfig buildHttpConfig(final int port, final String address){
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acc_conf = new AcceptorConfig();
        acc_conf.port = port;
        acc_conf.address = address;
        acc_conf.reusePort = true;
        //acc_conf.threads = java.lang.Thread.activeCount();
        //acc_conf.deferAccept = true;
        config.acceptors = new AcceptorConfig[]{acc_conf};
        return config;
    }
}
