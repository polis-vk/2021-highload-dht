package ru.mail.polis.service.avightclav;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceServer extends HttpServer implements ru.mail.polis.service.ServiceServer {
    private static final String PROTOCOL_ADDRESS_PREFIX = "http://localhost:";

    private final ExecutorService executor = Executors.newWorkStealingPool();
    private final ConsistentHashing hasher;
    private int number;
    private final NavigableMap<Integer, Service> services = new ConcurrentSkipListMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(ServiceServer.class);

    public ServiceServer(final int port, final DAO dao, final Set<String> topology) throws IOException {
        super(from(port));
        this.hasher = new ConsistentHashing(topology.size());

        final String myAddress = ServiceServer.PROTOCOL_ADDRESS_PREFIX + port;
        final int myAddressHash = myAddress.hashCode();
        final NavigableMap<Integer, String> topologyHashes = new TreeMap<>();
        for (String node: topology) {
            topologyHashes.put(node.hashCode(), node);
        }

        int index = 0;
        for (Map.Entry<Integer, String> node: topologyHashes.entrySet()) {
            if (node.getKey() == myAddressHash) {
                this.number = index;
                this.services.put(index, new LocalService(dao));
            } else {
                this.services.put(index, new RemoteService(node.getValue()));
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
                session.sendResponse(this.services.get(this.number).status());
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
                session.sendResponse(this.services.get(clusterNumber).get(id));
                break;
            case Request.METHOD_PUT:
                session.sendResponse(this.services.get(clusterNumber).put(id, request.getBody()));
                break;
            case Request.METHOD_DELETE:
                session.sendResponse(this.services.get(clusterNumber).delete(id));
                break;
            default:
                session.sendResponse(new Response(
                        Response.METHOD_NOT_ALLOWED,
                        "Wrong method".getBytes(StandardCharsets.UTF_8))
                );
                break;
        }
    }

    protected static byte[] extractBytes(final ByteBuffer byteBuffer) {
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.get(buffer);
        return buffer;
    }
}
