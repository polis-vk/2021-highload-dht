package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;

public class ServiceImpl extends HttpServer implements Service {

    private static final Logger systemlog = LoggerFactory.getLogger(ServiceImpl.class);
    private final ServiceDAO servDAO;
    private final ThreadPoolExecutor executor;

    /**
     * some doc.
     */
    public ServiceImpl(final ServiceConfig servConf, final ThreadPoolConfig tpc, final DAO dao) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(servConf));
        //this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(tpc.poolSize);
        BlockingQueue<Runnable> threadQueue = new LinkedBlockingDeque<>(tpc.queueSize);
        this.executor = new ThreadPoolExecutor(tpc.poolSize, tpc.MAX_POOL, tpc.keepAlive, tpc.unit, threadQueue);
        this.servDAO = new ServiceDAO(dao);
    }

    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        this.executor.execute(() -> {
            final HttpSession localSession = session;
            final String path = request.getPath();
            final int method = request.getMethod();
            try {
                switch (path) {
                    case "/v0/status":
                        localSession.sendResponse(status());
                        break;
                    case "/v0/entity":
                        Response resp = handleEntity(method, request);
                        localSession.sendResponse(resp);
                        break;
                    default:
                        localSession.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                        break;
                }
            } catch (IOException e) {
                systemlog.error("Error handle request: " + e.getMessage());
                return;
            }
        });
    }

    /**
     * some doc.
     */
    public Response handleEntity(final int method, final Request request) throws IOException {
        String id = "";
        final Iterator<String> params = request.getParameters("id");
        if (params.hasNext()) {
            id = params.next().substring(1);
        }

        try {
            if (id.isEmpty()) {
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
            } else {
                return servDAO.handleRequest(method, id, request);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Bad request", e);
        }
    }

}
