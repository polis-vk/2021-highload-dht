package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.sachuk.ilya.replication.ReplicationInfo;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNodeConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

public class ServiceImpl extends HttpServer implements Service {
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private final EntityRequestHandler entityRequestHandler;
    private final RequestPoolExecutor requestPoolExecutor = new RequestPoolExecutor(
            new ExecutorConfig(16, 1000)
    );
    private final NodeManager nodeManager;
    private final NodeRouter nodeRouter;
    private final Node node;
    private final Set<String> topology;

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws IOException {
        super(configFrom(port));

        this.topology = topology;
        this.entityRequestHandler = new EntityRequestHandler(dao);
        this.node = new Node(port);
        this.nodeManager = new NodeManager(topology, new VNodeConfig(), node);
        this.nodeRouter = new NodeRouter(nodeManager);
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }

    private Response entityRequest(Request request, String id) {

        if (id == null || id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
//        request.addHeader();

        Response response = nodeRouter.route(node, id, request);
        if (response != null) {
            return response;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return entityRequestHandler.get(id);
            case Request.METHOD_PUT:
                return entityRequestHandler.put(id, request);
            case Request.METHOD_DELETE:
                return entityRequestHandler.delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
    }

    private Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        if (requestPoolExecutor.isQueueFull()) {
            try {
                session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        requestPoolExecutor.addTask(() -> {
            String path = request.getPath();
            Response response;
            switch (path) {
                case ENTITY_PATH:
                    String id = request.getParameter("id=");
                    String replicas = request.getParameter("replicas=");

                    ReplicationInfo replicationInfo = replicas == null
                            ? ReplicationInfo.from(topology.size())
                            : ReplicationInfo.from(replicas);

                    logger.info(replicas);
                    logger.info("ask=" + replicationInfo.ask + " and from=" + replicationInfo.from);

                    if (replicationInfo.ask > replicationInfo.from || replicationInfo.ask == 0) {
                        response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                        break;
                    }

                    response = entityRequest(request, id);
                    break;
                case STATUS_PATH:
                    response = status();
                    break;
                default:
                    try {
                        handleDefault(request, session);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return;
            }
            try {
                session.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public synchronized void stop() {
        super.stop();

        nodeManager.close();
    }
}
