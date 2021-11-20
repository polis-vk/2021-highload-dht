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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
                        resp = handleEntity(request);
                        session.sendResponse(resp);
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
                LOG.error(e.getMessage());
            }
        });
    }

    @Override
    public synchronized void stop() {
        clusterService.stop();
        super.stop();
    }

    private Map<String, String> parseParameters(final Request request) {
        Map<String, String> parameters = new HashMap<>();
        String id = getParamRequest(request, "id");
        parameters.put("id", id);
        String ackFrom = getParamRequest(request, "replicas");
        if (ackFrom.isEmpty()) {
            parameters.put("ack", String.valueOf(clusterService.getQuorumCluster()));
            parameters.put("from", String.valueOf(clusterService.getClusterSize()));
        } else {
            String[] ackfrom = ackFrom.split("/");
            parameters.put("ack", ackfrom[0]);
            parameters.put("from", ackfrom[1]);
        }

        return parameters;
    }

    /**
     * some doc.
     */
    public Response handleEntity(final Request request) throws IOException {
        try {
            Map<String, String> params = parseParameters(request);
            return clusterService.handleRequest(request, params);
        } catch (IOException e) {
            throw new UncheckedIOException("Bad request", e);
        }
    }

    /**
     * some doc.
     */
    public Response internalRequest(final Request request) throws IOException {
        try {
            String id = getParamRequest(request, "id");
            return clusterService.internalRequest(request, id);
        } catch (IOException e) {
            throw new UncheckedIOException("Bad request", e);
        }
    }

    private String getParamRequest(final Request request, final String nameParam) {
        Iterator<String> params = request.getParameters(nameParam);
        return params.hasNext() ? params.next().substring(1) : "";
    }
}
