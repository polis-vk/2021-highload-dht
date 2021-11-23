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
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ServiceImpl extends HttpServer implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceImpl.class);
    private final ThreadPoolExecutor executor;
    private final ClusterService clusterService;

    public static byte[] BAD_REQUEST = "Bad request".getBytes(StandardCharsets.UTF_8);

    /**
     * some doc.
     */
    public ServiceImpl(final ServiceConfig servConf, final Set<String> topology, final DAO dao) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(servConf));
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(servConf.poolSize);
        this.clusterService = new ClusterService(dao, topology, servConf);
    }

    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        this.executor.execute(() -> {
            final String path = request.getPath();
            Response resp;
            try {
                switch (path) {
                    case "/v0/status":
                        session.sendResponse(status());
                        break;
                    case "/v0/entity":
                        handleEntity(request, session);
                        break;
                    case "/internal/cluster/entity":
                        resp = internalRequest(request);
                        session.sendResponse(resp);
                        break;
                    default:
                        resp = new Response(Response.BAD_REQUEST, Response.EMPTY);
                        session.sendResponse(resp);
                        break;
                }
            } catch (IOException e) {
                LOG.error("Error in handle entity {}", e.getMessage());
            }
        });
    }

    /**
     * some doc.
     */
    public void handleEntity(final Request request, final HttpSession session) throws IOException {
        try {
            RequestParameters params = new RequestParameters(request, clusterService);
            clusterService.handleRequest(session, params);
        } catch (IOException e) {
            throw new UncheckedIOException("Bad request", e);
        }
    }

    /**
     * some doc.
     */
    public Response internalRequest(final Request request) throws IOException {
        try {
            RequestParameters params = new RequestParameters(request, clusterService);
            return clusterService.internalRequest(request, params.getId());
        } catch (IOException e) {
            throw new UncheckedIOException("Bad request", e);
        }
    }

}
