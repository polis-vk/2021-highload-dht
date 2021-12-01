package ru.mail.polis.service.shabinsky_dmitry;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {

    public static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request: MySecretKey";
    public static final String TIMESTAMP_HEADER = "X-Entity-Timestamp: ";

    private static final int TIMEOUT = 1000;  // TODO config
    private static final Logger log = LoggerFactory.getLogger(BasicService.class); // TODO use lookup
    private final DAO dao;
    private final Executor executor;
    private final List<String> topology;
    private final Map<String, HttpClient> clients;

    public BasicService(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(16);
        this.topology = new ArrayList<>(topology);
        this.clients = extractSelfFromInterface(port, topology);
        Collections.sort(this.topology);
    }

    private static Map<String, HttpClient> extractSelfFromInterface(int port, Set<String> topology) throws UnknownHostException {
        // own addresses
        Map<String, HttpClient> clients = new HashMap<>();
        List<InetAddress> allMe = Arrays.asList(InetAddress.getAllByName(null));

        for (String node : topology) {
            ConnectionString connection = new ConnectionString(node);

            List<InetAddress> nodeAddresses = new ArrayList<>(Arrays.asList(
                InetAddress.getAllByName(connection.getHost()))
            ); // TODO optimize
            nodeAddresses.retainAll(allMe);

            if (nodeAddresses.isEmpty() || connection.getPort() != port) {
                HttpClient client = new HttpClient(connection);
                client.setReadTimeout(TIMEOUT);
                clients.put(node, client);
            }
        }
        return clients;
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status() {
        return Response.ok("I'm OK");
    }

    private Response mergeForExpectedCode(List<Response> responses, int expectedCode, int ackCount, String answer) {
        log.info("ackCount: {}", ackCount);

        for (Response response : responses) {
            if (response.getStatus() != expectedCode) {
                log.info("wrongStatus: {}", response.getHeader(INTERNAL_REQUEST_HEADER));
                // TODO some metrics?
                continue;
            }

            ackCount--;
            if (ackCount == 0) {
                return new Response(answer, Response.EMPTY);
            }
        }

        log.info("ackCountRemaining: {}", ackCount);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response selectBetterValue(Response previous, Response candidate) {
        if (previous == null || previous.getBody() == null) {
            return candidate;
        }

        if (candidate.getBody() == null) {
            return previous;
        }

        return Arrays.compare(previous.getBody(), candidate.getBody()) > 0
            ? candidate
            : previous;
    }

    private long extractTimestamp(Response response) {
        String header = response.getHeader(TIMESTAMP_HEADER);
        log.info("header {}", header);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private long extractTimestamp(Request request) {
        String header = request.getHeader(TIMESTAMP_HEADER);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private Response mergeForTimestamp(List<Response> responses, int ackCount) {
        Response best = null;
        long bestTimestamp = Long.MIN_VALUE;
        for (Response response : responses) {
            switch (response.getStatus()) {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_NOT_FOUND:
                    ackCount--;

                    long candidateTimestamp = extractTimestamp(response);
                    log.info("timestamp merge {}/{}", bestTimestamp, candidateTimestamp);
                    if (candidateTimestamp == bestTimestamp) {
                        best = selectBetterValue(best, response);
                    } else if (candidateTimestamp > bestTimestamp) {
                        best = response;
                        bestTimestamp = candidateTimestamp;
                    }
                    break;
                default:
            }
        }

        if (best == null || ackCount > 0) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        return best;
    }

    @Path("/v0/entity")
    public void entity(
        HttpSession session,
        Request request,
        @Param(value = "id", required = true) String id,
        @Param(value = "replicas") String replicas
    ) {
        if (id.isEmpty()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Id is empty")));
            return;
        }

        Context context;
        try {
            context = new Context(request, id, replicas);
        } catch (UnsupportedOperationException e) {
            log.warn("Wrong method", e);
            sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Utf8.toBytes("Wrong method")));
            return;
        } catch (IllegalArgumentException e) {
            log.warn("Wrong arguments", e);
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Wrong arguments")));
            return;
        }

        execute(session, context);
    }

    private Replicas extractReplicas(String replicas) {
        int ack;
        int from;
        if (replicas != null) {
            String[] parsed = replicas.split("/");
            ack = Integer.parseInt(parsed[0]);
            from = Integer.parseInt(parsed[1]);
        } else {
            from = topology.size();
            ack = from / 2 + 1;
        }

        if (ack > from || from <= 0 || from > topology.size() || ack <= 0) {
            throw new IllegalArgumentException("Replicas wrong");
        }

        return new Replicas(ack, from);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public synchronized void stop() {
        super.stop();
        for (HttpClient client : clients.values()) {
            client.close();
        }
    }

    private Response delete(String id, long tombstone) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Record record = Record.tombstone(key, tombstone);
        dao.upsert(record);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body, long tombstone) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        ByteBuffer value = ByteBuffer.wrap(body);
        Record record = Record.of(key, value, tombstone);
        dao.upsert(record);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response okForGet(Record record) {
        Response response;
        if (record.isTombstone()) {
            response = new Response(Response.NOT_FOUND, Response.EMPTY);
        } else {
            response = new Response(Response.OK, extractBytes(record.getValue()));
        }

        response.addHeader(TIMESTAMP_HEADER + record.getTimestamp());
        return response;
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key), true);
        if (range.hasNext()) {
            return okForGet(range.next());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private List<Node> collectNodesForKey(String id, int nodeCount) {
        PriorityQueue<Node> nodes = new PriorityQueue<>(Comparator.comparingInt(o -> o.hash));

        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            HttpClient client = clients.get(option);
            Node node = new Node(hash, client);
            nodes.add(node);

            if (nodes.size() > nodeCount) {
                nodes.remove();
            }
        }

        return new ArrayList<>(nodes);
    }

    private void execute(HttpSession session, Context context) {
        executor.execute(() -> {
            Response result;
            try {
                result = context.isCoordinator
                    ? collectResponses(context)
                    : invokeLocalRequest(context);
            } catch (Exception e) {
                log.error("Unexpected exception during method call {}", context.operation, e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Something wrong")));
                return;
            }

            sendResponse(session, result);
        });
    }

    private Response invokeLocalRequest(Context context) {
        switch (context.operation) {
            case GET:
                return get(context.id);
            case UPSERT:
                return put(context.id, context.payload, context.timestamp);
            case DELETE:
                return delete(context.id, context.timestamp);
            default:
                throw new UnsupportedOperationException("Unsupported operation " + context.operation);
        }
    }

    private Response invokeRemoteRequest(Context context, Node node) throws HttpException, IOException, PoolException, InterruptedException {
        switch (context.operation) {
            case GET:
                return node.client.get(
                    context.uri,
                    INTERNAL_REQUEST_HEADER);
            case UPSERT:
                return node.client.put(
                    context.uri,
                    context.payload,
                    INTERNAL_REQUEST_HEADER,
                    TIMESTAMP_HEADER + context.timestamp);
            case DELETE:
                return node.client.delete(
                    context.uri,
                    INTERNAL_REQUEST_HEADER,
                    TIMESTAMP_HEADER + context.timestamp);
            default:
                throw new UnsupportedOperationException("Unsupported operation " + context.operation);
        }
    }

    private Response collectResponses(Context context) throws InterruptedIOException {
        List<Node> nodes = collectNodesForKey(context.id, context.replicas.from);
        List<Response> responses = new ArrayList<>();

        for (Node node : nodes) {
            if (node.isLocaleNode()) {
                Response localResult = invokeLocalRequest(context);
                responses.add(localResult);
            } else {
                try {
                    Response remoteResult = invokeRemoteRequest(context, node);
                    responses.add(remoteResult);
                } catch (HttpException | IOException | PoolException e) {
                    log.warn("Exception during node communication", e);
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
        return merge(context, responses);
    }

    private Response merge(Context context, List<Response> responses) {
        switch (context.operation) {
            case GET:
                return mergeForTimestamp(responses, context.replicas.ack);
            case UPSERT:
                return mergeForExpectedCode(
                    responses,
                    HttpURLConnection.HTTP_CREATED,
                    context.replicas.ack,
                    Response.CREATED
                );
            case DELETE:
                return mergeForExpectedCode(
                    responses,
                    HttpURLConnection.HTTP_ACCEPTED,
                    context.replicas.ack,
                    Response.ACCEPTED
                );
            default:
                throw new UnsupportedOperationException("Unsupported operation " + context.operation);
        }
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    private static class Node {
        final int hash;
        final HttpClient client;

        Node(int hash, HttpClient client) {
            this.hash = hash;
            this.client = client;
        }

        boolean isLocaleNode() {
            return client == null;
        }
    }

    private class Context {

        final String id;
        final boolean isCoordinator;
        final long timestamp;
        final public byte[] payload;
        final Operation operation;
        final String uri;

        final Replicas replicas;

        Context(Request request, String id, String replicas) {
            this.id = id;
            this.isCoordinator = !"".equals(request.getHeader(INTERNAL_REQUEST_HEADER));
            this.timestamp = isCoordinator
                ? System.currentTimeMillis()
                : extractTimestamp(request);
            this.payload = request.getBody();
            this.uri = request.getURI();

            this.replicas = isCoordinator
                ? extractReplicas(replicas)
                : null;

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    this.operation = Operation.GET;
                    break;
                case Request.METHOD_PUT:
                    this.operation = Operation.UPSERT;
                    break;
                case Request.METHOD_DELETE:
                    this.operation = Operation.DELETE;
                    break;
                default:
                    throw new UnsupportedOperationException("Method " + request.getMethod() + " is not supported");
            }
        }
    }

    private static class Replicas {
        final int ack;
        final int from;

        Replicas(int ack, int from) {
            this.ack = ack;
            this.from = from;
        }
    }

    private enum Operation {
        GET, UPSERT, DELETE
    }
}
