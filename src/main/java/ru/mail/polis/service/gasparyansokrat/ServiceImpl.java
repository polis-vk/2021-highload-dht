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

    public static final int STATUS_OK = 200;
    public static final int STATUS_NOT_FOUND = 404;
    public static final int STATUS_BAD_GATEWAY = 502;
    public static final int STATUS_CREATED = 201;
    public static final int STATUS_DELETED = 202;

    /**
     * some doc.
     */
    public ServiceImpl(final ServiceConfig servConf, final Set<String> topology, final DAO dao) throws IOException {
        super(HttpConfigFactory.buildHttpConfig(servConf));
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(servConf.poolSize);
        this.clusterService = new ClusterService(dao, topology, servConf);
    }

    @Override
    public void stop() {
        clusterService.stop();
        super.stop();
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

    private Map<String, String> parseParameters(final Request request) {
        Map<String, String> parameters = new HashMap<>();
        String id = getParamRequest(request, "id");
        if (id.isEmpty()) {
            return null;
        }
        parameters.put("id", id);
        String ackFrom = getParamRequest(request, "replicas");
        if (!ackFrom.isEmpty()) {
            String[] ackfrom = ackFrom.split("/");
            final int numNodes = Integer.parseInt(ackfrom[0]);
            final int maxNodes = Integer.parseInt(ackfrom[1]);
            if (numNodes > maxNodes) {
                return null;
            }
            parameters.put("ack", ackfrom[0]);
            parameters.put("from", ackfrom[1]);
        } else {
            parameters.put("ack", String.valueOf(clusterService.getQuorumCluster()));
            parameters.put("from", String.valueOf(clusterService.getClusterSize()));
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
        String param = "";
        Iterator<String> params = request.getParameters(nameParam);
        if (params.hasNext()) {
            param = params.next().substring(1);
        }
        return param;
    }
}
