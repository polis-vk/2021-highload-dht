package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ServiceImpl extends HttpServer implements Service {

    private final ServiceDAO servDAO;
    private final ThreadPoolExecutor executor;

    /**
     * some doc.
     */
    public ServiceImpl(final int port, final DAO dao, final int poolSize) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(port, "localhost"));
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);
        this.servDAO = new ServiceDAO(dao);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        this.executor.execute(() -> {
            try {
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

    }

    /**
     * some doc.
     */
    @Path("/v0/entity")
    public void handleEntity(Request req, HttpSession session,
                                @Param(value = "id", required = true) String id) throws IOException {

        executor.execute(() -> {
            Response resp = null;
            try {
                if (id.isEmpty()) {
                    resp = new Response(Response.BAD_REQUEST, Response.EMPTY);
                } else {
                    resp = servDAO.handleRequest(req, id);
                }
                session.sendResponse(resp);
            } catch (IOException e) {
                throw new UncheckedIOException("Bad request", e);
            }
        });
    }
}
