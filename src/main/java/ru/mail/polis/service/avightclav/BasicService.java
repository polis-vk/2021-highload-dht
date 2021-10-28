package ru.mail.polis.service.avightclav;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
//    private final static Pattern PROTOCOL_PATTERN = Pattern.compile("^(\\w+)://([^:/]*)(?::(\\d+))?.*");
    private final static String PROTOCOL_ADDRESS_PREFIX = "http://localhost:";

    private final DAO dao;
    private final ExecutorService executor = Executors.newWorkStealingPool();
    private final ConsistentHashing hasher;
    private int number;
    private final NavigableMap<Integer, RemoteService> remoteServices = new ConcurrentSkipListMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(BasicService.class);

    public BasicService(final int port, final DAO dao, final Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.hasher = new ConsistentHashing(topology.size());
        System.out.println("Starting server with port" + port);

        final String myAddress = BasicService.PROTOCOL_ADDRESS_PREFIX + port;
        final int myAddressHash = myAddress.hashCode();
        final NavigableMap<Integer, String> topologyHashes = new TreeMap<>();
        for (String node: topology) {
            topologyHashes.put(node.hashCode(), node);
        }

        int index = 0;
        for (Map.Entry<Integer, String> node: topologyHashes.entrySet()) {
            if (node.getKey() == myAddressHash) {
                this.number = index;
            } else {
                this.remoteServices.put(index, new RemoteService(node.getValue(), myAddress, node.getValue()));
            }
            index++;
        }
    }

    protected static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        executor.execute(() -> {
            try {
                pathWiring(request, session);
            } catch (IOException e) {
                LOG.error("Request handling IO exception", e);
            }
        });
    }

    private void pathWiring(Request request, HttpSession session) throws IOException {
        String path = request.getPath();
        switch (path) {
            case "/v0/status":
                if (request.getMethod() == Request.METHOD_GET) {
                    session.sendResponse(this.status());
                }
                break;
            case "/v0/entity":
                entityWiring(request, session);
                break;
            default:
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                break;
        }
    }

    private void entityWiring(Request request, HttpSession session) throws IOException {
        final String id = request.getParameter("id", "=");
        if ("=".equals(id)) {
            session.sendResponse(new Response(Response.BAD_REQUEST, "Wrong id".getBytes()));
            return;
        }
        int clusterNumber = this.hasher.getClusterId(id);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (this.number == clusterNumber) {
                    session.sendResponse(this.get(id));
                } else {
                    session.sendResponse(this.remoteServices.get(clusterNumber).getEntity(request));
                }
                break;
            case Request.METHOD_PUT:
                if (this.number == clusterNumber) {
                    session.sendResponse(this.put(id, request.getBody()));
                } else {
                    session.sendResponse(this.remoteServices.get(clusterNumber).put(request));
                }
                break;
            case Request.METHOD_DELETE:
                if (this.number == clusterNumber) {
                    session.sendResponse(this.delete(id));
                } else {
                    session.sendResponse(this.remoteServices.get(clusterNumber).delete(request));
                }
                break;
            default:
                session.sendResponse(new Response(
                        Response.METHOD_NOT_ALLOWED,
                        "Wrong method".getBytes(StandardCharsets.UTF_8))
                );
                break;
        }
    }

    public Response status() {
        return Response.ok("I'm ok");
    }

    public Response entity(
            final Request request,
            @Param(value = "id", required = true) final String id) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        int clusterNumber = this.hasher.getClusterId(id);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (clusterNumber == this.number) {
                    return this.get(id);
                } else {
                    this.remoteServices.get(clusterNumber).getEntity(request);
                }
            case Request.METHOD_PUT:
                if (clusterNumber == this.number) {
                    return this.put(id, request.getBody());
                } else {
                    this.remoteServices.get(clusterNumber).getEntity(request);
                }
            case Request.METHOD_DELETE:
                if (clusterNumber == this.number) {
                    return this.delete(id);
                } else {
                    this.remoteServices.get(clusterNumber).getEntity(request);
                }
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Wrong method".getBytes(StandardCharsets.UTF_8));
        }
    }

    protected static byte[] extractBytes(final ByteBuffer byteBuffer) {
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.get(buffer);
        return buffer;
    }

    private Response get(final String id) {
        final ByteBuffer keyFrom = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final Iterator<Record> range = this.dao.range(keyFrom, DAO.nextKey(keyFrom));
        if (range.hasNext()) {
            final Record record = range.next();
            return new Response(Response.OK, BasicService.extractBytes(record.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(final String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final String id) {
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
