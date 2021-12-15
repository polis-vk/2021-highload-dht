package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.sachuk.ilya.replication.Coordinator;
import ru.mail.polis.service.sachuk.ilya.replication.ReplicationInfo;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNodeConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Set;

public class ServiceImpl extends HttpServer implements Service {
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITIES_PATH = "/v0/entities";

    private final EntityRequestHandler entityRequestHandler;
    private final ConfiguredPoolExecutor requestPoolExecutor = new ConfiguredPoolExecutor(
            new ExecutorConfig(8, 1000)
    );

    private final NodeManager nodeManager;
    private final Node node;
    private final Set<String> topology;
    private final Coordinator coordinator;

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws IOException {
        super(configFrom(port));

        this.topology = topology;
        this.entityRequestHandler = new EntityRequestHandler(dao);
        this.node = new Node("http", "localhost", port, "http://localhost:" + port);
        this.nodeManager = new NodeManager(topology, new VNodeConfig());
        NodeRouter nodeRouter = new NodeRouter();

        this.coordinator = new Coordinator(nodeManager, nodeRouter, entityRequestHandler, node);

        logger.info("Node with port {} is started", port);
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }

    private void entityRequest(Request request, String id, ReplicationInfo replicationInfo, HttpSession session) {
        Response response = null;
        if (id == null || id.isEmpty()) {
            response = new Response(Response.BAD_REQUEST, Response.EMPTY);

            try {
                session.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        boolean isCoordinator = false;
        String timestamp = request.getHeader(ResponseUtils.TIMESTAMP_HEADER);
        if (timestamp == null) {
            isCoordinator = true;
            request.addHeader(ResponseUtils.TIMESTAMP_HEADER + System.currentTimeMillis());
        }

        if (isCoordinator) {
            logger.info("in block is coordinator");
            coordinator.handle(replicationInfo, id, request, session);
        } else {
            logger.info("in block from coordinator");
            response = entityRequestHandler.handle(request, id);
        }

        if (response == null) {
            return;
        }

        try {
            session.sendResponse(response);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public HttpSession createSession(Socket socket) throws RejectedSessionException {
        return new ChunkedHttpSession(socket, this);
    }

    public void rangeEntities(String start, String end, HttpSession session) {
        if (start == null || start.isEmpty()) {
            try {
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        logger.info("In ENTITIES");
        Response response = new Response(Response.OK);
        Iterator<Record> entityRange = entityRequestHandler.getEntitiesRange(start, end);

        ChunkedHttpSession chunkedHttpSession = (ChunkedHttpSession) session;

        logger.info("AFTER SET ITERATOR");
        try {
            chunkedHttpSession.sendResponseWithRange(response, () -> entityRange.hasNext()
                    ? entityRange.next()
                    : null
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

        requestPoolExecutor.execute(() -> {
            try {
                String path = request.getPath();
                Response response;
                switch (path) {
                    case ENTITY_PATH:
                        String id = request.getParameter("id=");
                        String replicas = request.getParameter("replicas=");

                        ReplicationInfo replicationInfo = replicas == null
                                ? ReplicationInfo.of(topology.size())
                                : ReplicationInfo.of(replicas);

                        if (replicationInfo.ask > replicationInfo.from || replicationInfo.ask == 0) {
                            response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                            break;
                        }

                        entityRequest(request, id, replicationInfo, session);
                        return;
                    case STATUS_PATH:
                        response = status();
                        break;
                    case ENTITIES_PATH:
                        String start = request.getParameter("start=");
                        String end = request.getParameter("end=");

                        rangeEntities(start, end, session);

                        return;
                    default:
                        handleDefault(request, session);
                        return;
                }
                session.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public synchronized void stop() {
        super.stop();

        logger.info("Service with node: {} is closed", node.port);
        nodeManager.close();
    }
}
