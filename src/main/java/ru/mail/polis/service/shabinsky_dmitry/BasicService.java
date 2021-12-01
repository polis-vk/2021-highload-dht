package ru.mail.polis.service.shabinsky_dmitry;

import one.nio.http.*;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class BasicService extends HttpServer implements Service {

    public static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    public static final String INTERNAL_REQUEST_HEADER_VALUE = "MySecretKey";
    public static final String INTERNAL_REQUEST_CHECK = INTERNAL_REQUEST_HEADER + ": " + INTERNAL_REQUEST_HEADER_VALUE;
    public static final String TIMESTAMP_HEADER = "X-Entity-Timestamp";
    public static final String TIMESTAMP_HEADER_FOR_READ = TIMESTAMP_HEADER + ": ";

    private static final int TIMEOUT = 1000;
    private static final Logger log = LoggerFactory.getLogger(BasicService.class);
    private final DAO dao;
    private final Executor executor;
    private final List<String> topology;
    private final String self;

    private final Executor httpExecutor = new ForkJoinPool(16);
    private final HttpClient client;

    public BasicService(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(16);
        this.topology = new ArrayList<>(topology);
        this.self = extractSelf(port, topology);
        Collections.sort(this.topology);

        client = HttpClient.newBuilder()
            .executor(httpExecutor)
            .connectTimeout(Duration.ofSeconds(1))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    private String extractSelf(int selfPort, Set<String> topology) {
        for (String option : topology) {
            try {
                int port = new URI(option).getPort();
                if (port == selfPort) {
                    return option;
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Wrong url", e);
            }
        }
        throw new IllegalArgumentException("Unknown port " + port);
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

    private RemoteData selectBetterValue(RemoteData previous, RemoteData candidate) {
        if (previous == null || previous.data == null) {
            return candidate;
        }

        if (candidate.data == null) {
            return previous;
        }

        return Arrays.compare(previous.data, candidate.data) > 0
            ? candidate
            : previous;
    }

    private long extractTimestamp(Request request) {
        String header = request.getHeader(TIMESTAMP_HEADER_FOR_READ);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private CompletableFuture<Response> mergeForExpectedCode(
        CompletableFuture<List<RemoteData>> responses,
        int expectedCode,
        int ackCount,
        String answer
    ) {
        return responses.thenApply(list -> {
            AtomicReference<Response> answerResponse = new AtomicReference<>(
                new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
            AtomicInteger askingCount = new AtomicInteger(ackCount);

            for (RemoteData response : list) {
                if (response.status != expectedCode) {
                    continue;
                }

                askingCount.getAndDecrement();
                if (askingCount.get() == 0) {
                    answerResponse.set(new Response(answer, Response.EMPTY));
                }
            }

            log.info("ackCountRemaining: {}", askingCount.get());
            return answerResponse.get();
        });
    }

    private CompletableFuture<Response> mergeForTimestamp(
        CompletableFuture<List<RemoteData>> responses,
        int ackCount
    ) {
        return responses.thenApply(list -> {
            AtomicReference<RemoteData> best = new AtomicReference<>(null);
            AtomicLong bestTimestamp = new AtomicLong(Long.MIN_VALUE);
            AtomicInteger askingCount = new AtomicInteger(ackCount);

            for (RemoteData response : list) {
                log.info("Resp: {}", response);
                switch (response.status) {
                    case HttpURLConnection.HTTP_OK:
                    case HttpURLConnection.HTTP_NOT_FOUND:
                        askingCount.getAndDecrement();

                        AtomicLong candidateTimestamp = new AtomicLong(response.timestamp == null
                            ? Long.MIN_VALUE
                            : response.timestamp);

                        if (candidateTimestamp.get() == bestTimestamp.get()) {
                            best.set(selectBetterValue(best.get(), response));
                        } else if (candidateTimestamp.get() > bestTimestamp.get()) {
                            best.set(response);
                            bestTimestamp.set(candidateTimestamp.get());
                        }
                        break;
                    default:
                }
            }

            if (best.get() == null || askingCount.get() > 0) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }

            return toResponse(best.get());
        });
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public CompletableFuture<List<RemoteData>> ackAndCollect(
        final List<CompletableFuture<RemoteData>> responsesList,
        final int ack
    ) {

        final AtomicInteger countError = new AtomicInteger(0);
        final List<RemoteData> collectedResponse = new CopyOnWriteArrayList<>();

        final CompletableFuture<List<RemoteData>> returnCF = new CompletableFuture<>();

        responsesList.forEach(responseCF -> responseCF.whenCompleteAsync((r, t) ->
        {
            if (t != null) {
                if (countError.incrementAndGet() == (responsesList.size() - ack + 1)) {
                    returnCF.complete(List.of());
                }
                return;
            }

            if (collectedResponse.size() <= ack) {
                collectedResponse.add(r);
                if (collectedResponse.size() == ack) {
                    returnCF.complete(collectedResponse);
                }
            }
        }, httpExecutor));

        return returnCF;
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

    @SuppressWarnings("StringSplitter")
    private Replicas extractReplicas(String replicas) {
        int ack;
        int from;
        if (replicas == null) {
            from = topology.size();
            ack = from / 2 + 1;
        } else {
            String[] parsed = replicas.split("/");
            ack = Integer.parseInt(parsed[0]);
            from = Integer.parseInt(parsed[1]);
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
    }

    private RemoteData delete(String id, long tombstone) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Record record = Record.tombstone(key, tombstone);
        dao.upsert(record);
        return new RemoteData(202, Response.EMPTY, null);
    }

    private RemoteData put(String id, byte[] body, long tombstone) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        ByteBuffer value = ByteBuffer.wrap(body);
        Record record = Record.of(key, value, tombstone);
        dao.upsert(record);
        return new RemoteData(201, Response.EMPTY, null);

    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private RemoteData okForGet(Record record) {
        if (record.isTombstone()) {
            return new RemoteData(404, null, record.getTimestamp());
        }

        return new RemoteData(
            200,
            extractBytes(record.getValue()),
            record.getTimestamp());
    }

    private RemoteData get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key), true);
        if (range.hasNext()) {
            return okForGet(range.next());
        } else {
            return new RemoteData(404, null, null);
        }
    }

    private List<Node> collectNodesForKey(String id, int nodeCount) {
        PriorityQueue<Node> nodes = new PriorityQueue<>(Comparator.comparingInt(o -> o.hash));

        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            Node node = new Node(hash, option.equals(self) ? null : option);
            nodes.add(node);

            if (nodes.size() > nodeCount) {
                nodes.remove();
            }
        }

        return new ArrayList<>(nodes);
    }

    private void execute(HttpSession session, Context context) {
        executor.execute(() -> {
            CompletableFuture<Response> result;
            try {
                result = context.isCoordinator
                    ? collectResponses(context)
                    : CompletableFuture.supplyAsync(() -> toInternalResponse(invokeLocalRequest(context)), httpExecutor);
            } catch (Exception e) {
                log.error("Unexpected exception during method call {}", context.operation, e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Something wrong")));
                return;
            }

            sendResponse(session, result);
        });
    }


    private CompletableFuture<RemoteData> invokeRemoteRequest(Context context, Node node) throws HttpException, IOException, PoolException, InterruptedException {
        URI uri = URI.create(node.uri + "/v0/entity?id=" + context.id);

        switch (context.operation) {
            case GET:
                HttpRequest get = HttpRequest.newBuilder(uri)
                    .GET()
                    .timeout(Duration.ofMillis(TIMEOUT))
                    .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                    .header(TIMESTAMP_HEADER, String.valueOf(context.timestamp))
                    .build();

                return client.sendAsync(get, HttpResponse.BodyHandlers.ofByteArray())
                    .thenApplyAsync(r -> {
                        String timeGet = r.headers().firstValue(TIMESTAMP_HEADER).orElse(null);

                        Long result = null;
                        if (!Objects.equals(timeGet, "null")) {
                            result = Long.valueOf(timeGet);
                        }

                        return new RemoteData(
                            r.statusCode(),
                            r.body(),
                            result);
                    }, httpExecutor);
            case UPSERT:
                HttpRequest put = HttpRequest.newBuilder(uri)
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(context.payload))
                    .timeout(Duration.ofMillis(TIMEOUT))
                    .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                    .header(TIMESTAMP_HEADER, String.valueOf(context.timestamp))
                    .build();

                return client.sendAsync(put, HttpResponse.BodyHandlers.ofByteArray())
                    .thenApplyAsync(r -> new RemoteData(
                        r.statusCode(),
                        r.body(),
                        null), httpExecutor);
            case DELETE:
                HttpRequest delete = HttpRequest.newBuilder(uri)
                    .DELETE()
                    .timeout(Duration.ofMillis(TIMEOUT))
                    .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                    .header(TIMESTAMP_HEADER, String.valueOf(context.timestamp))
                    .build();

                return client.sendAsync(delete, HttpResponse.BodyHandlers.ofByteArray())
                    .thenApplyAsync(r -> new RemoteData(
                        r.statusCode(),
                        r.body(),
                        null), httpExecutor);
            default:
                throw new UnsupportedOperationException("Unsupported operation " + context.operation);
        }
    }

    private RemoteData invokeLocalRequest(Context context) {
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

    private CompletableFuture<Response> collectResponses(Context context) throws InterruptedIOException {
        List<Node> nodes = collectNodesForKey(context.id, context.replicas.from);
        List<CompletableFuture<RemoteData>> responses = new ArrayList<>();

        for (Node node : nodes) {
            if (node.isLocaleNode()) {
                CompletableFuture<RemoteData> localResult = CompletableFuture.supplyAsync(() -> invokeLocalRequest(context), httpExecutor);
                responses.add(localResult);
            } else {
                try {
                    CompletableFuture<RemoteData> remoteResult = invokeRemoteRequest(context, node);
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

    private CompletableFuture<Response> merge(Context context, List<CompletableFuture<RemoteData>> responses) {
        CompletableFuture<List<RemoteData>> collectResponse = ackAndCollect(responses, context.replicas.ack);

        switch (context.operation) {
            case GET:
                return mergeForTimestamp(collectResponse, context.replicas.ack);
            case UPSERT:
                return mergeForExpectedCode(
                    collectResponse,
                    HttpURLConnection.HTTP_CREATED,
                    context.replicas.ack,
                    Response.CREATED
                );
            case DELETE:
                return mergeForExpectedCode(
                    collectResponse,
                    HttpURLConnection.HTTP_ACCEPTED,
                    context.replicas.ack,
                    Response.ACCEPTED
                );
            default:
                throw new UnsupportedOperationException("Unsupported operation " + context.operation);
        }
    }

    private String statusToString(int status) {
        switch (status) {
            case 200:
                return Response.OK;
            case 201:
                return Response.CREATED;
            case 202:
                return Response.ACCEPTED;
            case 404:
                return Response.NOT_FOUND;
            default:
                throw new IllegalArgumentException("Unknown status " + status);
        }
    }

    private void sendResponse(HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    private void sendResponse(HttpSession session, CompletableFuture<Response> response) {
        if (response.whenComplete((r, t) -> {
            if (t == null) {
                try {
                    session.sendResponse(r);
                } catch (IOException e) {
                    log.info("Can't send response", e);
                }
            } else {
                try {
                    session.sendError(Response.INTERNAL_ERROR, t.getMessage());
                } catch (IOException e) {
                    log.info("Can't send error", e);
                }
            }
        }).isCancelled()) {
            log.info("Can't send response !!!");
        }
    }

    private Response toResponse(RemoteData data) {
        return new Response(
            statusToString(data.status),
            data.data == null ? Response.EMPTY : data.data);
    }

    private Response toInternalResponse(RemoteData data) {
        Response response = toResponse(data);
        response.addHeader(TIMESTAMP_HEADER + ": " + data.timestamp);
        return response;
    }

    private static class Node {
        final int hash;
        final String uri;

        Node(int hash, String uri) {
            this.hash = hash;
            this.uri = uri;
        }

        boolean isLocaleNode() {
            return uri == null;
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
            this.isCoordinator = !"".equals(request.getHeader(INTERNAL_REQUEST_CHECK));
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

    private static class RemoteData {
        final int status;
        final byte[] data;
        final Long timestamp;

        public RemoteData(int status, byte[] data, Long timestamp) {
            this.status = status;
            this.data = data;
            this.timestamp = timestamp;
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
