package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;

public class ServiceImpl extends HttpServer implements Service {

    public ServiceImpl(int port, DAO dao) throws IOException {
        super(configFrom(port));
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        super.handleDefault(request, session);
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }
}
