package ru.mail.polis.service;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.controller.MainController;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.artem_drozdov.DAOState;

import java.io.IOException;

public class HttpServerImpl extends HttpServer implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServerImpl.class);

    private final DAO dao;

    public HttpServerImpl(final int port,
                          final DAO dao) throws IOException {
        super(getConfig(port), new MainController(dao));
        this.dao = dao;
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
    public void handleDefault(Request request, HttpSession session) throws IOException {

        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        DAOState daoState = dao.getState();
        if (daoState != DAOState.OK) {
            session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY));
            LOG.warn("Server rejected response from: {} because of DAOState: {}",
                    session.getRemoteHost(), daoState);
            return;
        }
        super.handleRequest(request, session);
    }
}
