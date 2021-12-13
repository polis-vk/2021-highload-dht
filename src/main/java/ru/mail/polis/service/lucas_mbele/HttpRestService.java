package ru.mail.polis.service.lucas_mbele;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class HttpRestService extends HttpServer implements Service {
    private final Logger logger = LoggerFactory.getLogger(HttpRestService.class);
    private final DAO dao;
    private final ThreadPoolExecutor threadPoolExecutorUtil;
    private final NodesClusterService nodesClusterService;

    public HttpRestService(final int port, final DAO dao, final Set<String> topology) throws IOException {
        super(serviceConfig(port));
        this.dao = dao;
        this.threadPoolExecutorUtil = (ThreadPoolExecutor) Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.nodesClusterService = new NodesClusterService(topology, port);
    }

    public static HttpServerConfig serviceConfig(int port) {
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ServiceUtils.acceptors(port)};
        return config;
    }

    public Response status() {
        return Response.ok(Response.OK);
    }

    public Response entity(Request request, @Param(value = "id", required = true) String id) {
        final int inquiredMethod = request.getMethod();
        Response response;
        if (id.isBlank()) {
            response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        } else {
                if (inquiredMethod == Request.METHOD_GET) {
                    response = get(id);
                } else if (inquiredMethod == Request.METHOD_PUT) {
                    response = put(id, request.getBody());
                } else if (inquiredMethod == Request.METHOD_DELETE) {
                    response = delete(id);
                } else {
                    response = new Response(Response.METHOD_NOT_ALLOWED,
                            "Method not allowed".getBytes(StandardCharsets.UTF_8));
                }
            }
        return response;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        this.threadPoolExecutorUtil.execute(() -> {
            String idRequest = "";
            final String path = request.getPath();
            final Iterator<String> idIterator = request.getParameters("id");
            final Response sentResponse;
            if (idIterator.hasNext()) {
                idRequest = idIterator.next().substring(1);
            }
            try {
                switch (path) {
                    case "/v0/status":
                        session.sendResponse(status());
                        break;
                    case "/v0/entity":
                        sentResponse = nodesClusterService.handleRequest(idRequest,request);
                        session.sendResponse(sentResponse);
                        break;
                    default:
                        handleDefault(request, session);
                        break;
                }
            } catch (IOException e) {
                logger.error(e.getLocalizedMessage());
            }
        });
    }

    @Override
    public void handleDefault(Request request, HttpSession session) {
        try {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Response get(String id) {
        Response response;
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> keyIterator = dao.range(key, DAO.nextKey(key));
        if (keyIterator.hasNext()) {
            Record record = keyIterator.next();
            response = new Response(Response.OK, ServiceUtils.extractBytesBuffer(record.getValue()));
        } else {
            response = new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return response;
    }

    private Response put(String id, byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

   public class NodesClusterService {
        private final Logger logger = LoggerFactory.getLogger(NodesClusterService.class);
        private final RendezvousHashingImpl rendezvousHashing;
        private final Map<String, HttpClient> nodesMap;
        private String currentNode;
        private final int port;

        NodesClusterService(final Set<String> topology, int port) {
            this.port = port;
            this.rendezvousHashing = new RendezvousHashingImpl(topology);
            nodesMap = new LinkedHashMap<>();
            buildTopology(topology);
        }

        public final void buildTopology(Set<String> topology) {
            for (String node : topology) {
                try {
                    URL url = new URL(node);
                    if (this.port == url.getPort()) {
                        currentNode = node;
                    } else {
                        nodesMap.put(node, new HttpClient(new ConnectionString(node)));
                    }
                } catch (MalformedURLException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
        }

        public Response handleRequest(String key, Request request) {
            String responsibleNode = rendezvousHashing.getResponsibleNode(key);
            Response response;
            if (currentNode.equals(responsibleNode)) {
                response = HttpRestService.this.entity(request,key);
            } else {
                response = Response.ok(Response.EMPTY);
                final String uri = "/v0/entity?id=";
                int inquiredMethod = request.getMethod();
                try {
                    if (inquiredMethod == Request.METHOD_PUT) {
                        response = nodesMap.get(responsibleNode).put(uri + key, request.getBody());
                    } else if (inquiredMethod == Request.METHOD_GET) {
                        response = nodesMap.get(responsibleNode).get(uri + key);
                    } else if (inquiredMethod == Request.METHOD_DELETE) {
                        response = nodesMap.get(responsibleNode).delete(uri + key);
                    } else {
                        response = new Response(Response.METHOD_NOT_ALLOWED,
                                "Method not allowed".getBytes(StandardCharsets.UTF_8));
                    }
                } catch (InterruptedException | PoolException | IOException | HttpException e) {
                    logger.error(e.getLocalizedMessage());
                }
            }
            return response;
        }
    }
}
