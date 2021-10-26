package ru.mail.polis.service;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.ClusterPartitioner;
import ru.mail.polis.controller.MainController;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.request.DefaultRequestHandler;

import java.io.IOException;

public class HttpServerImpl extends HttpServer implements Service {

    private final RequestHandler requestHandler;
    private final ClusterPartitioner clusterPartitioner;

    public HttpServerImpl(final int port, final DAO dao, final ClusterPartitioner clusterPartitioner) throws IOException {
        super(getConfig(port));

        this.clusterPartitioner = clusterPartitioner;
        this.requestHandler = new DefaultRequestHandler(new MainController(dao));
    }

    private static HttpServerConfig getConfig(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};

        return config;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        requestHandler.handleRequest(request, session);
    }
}
