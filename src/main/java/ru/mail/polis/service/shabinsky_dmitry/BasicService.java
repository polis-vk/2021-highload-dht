package ru.mail.polis.service.shabinsky_dmitry;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public final class BasicService extends HttpServer implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(BasicService.class);
    private static final int TIMEOUT = 1000; // TODO config
    private final DAO dao;
    private final ExecutorService executor;
    private final List<String> topology;

    public BasicService(
        final int port,
        final DAO dao,
        Set<String> topology
    ) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(128, r -> new MyThread(r, port, topology));
        this.topology = new ArrayList<>(topology);
        Collections.sort(this.topology);
    }

    private static Map<String, HttpClient> extractSelfFromInterface(int port, Set<String> topology) throws UnknownHostException {
        Map<String, HttpClient> clients = new ConcurrentHashMap<>();
        List<InetAddress> allMe = Arrays.asList(InetAddress.getAllByName(null));

        for (String node : topology) {
            ConnectionString connection = new ConnectionString(node +
                "?clientMinPoolSize=1&clientMaxPoolSize=1");

            List<InetAddress> nodeAddresses = Arrays.asList(
                InetAddress.getAllByName(connection.getHost())
            );
            nodeAddresses.retainAll(allMe);

            if (nodeAddresses.isEmpty() || connection.getPort() != port) {
                clients.put(node, new HttpClient(connection));
            }
        }
        return clients;
    }

    private static HttpServerConfig from(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("I'm ok");
    }

    @Path("/v0/entity")
    public void entity(
        HttpSession session,
        Request request,
        @Param(value = "id", required = true) final String id,
        @Param("replicas") String replicas
    ) {
        execute(id, replicas, request, session, () -> {

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(id);
                case Request.METHOD_PUT:
                    return put(id, request.getBody());
                case Request.METHOD_DELETE:
                    return delete(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, toBytes("Wrong method"));
            }
        });
    }

    @Override
    public void handleDefault(
        Request request,
        HttpSession session
    ) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }


    private Response delete(String id) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        Record record = Record.tombstone(key);
        dao.upsert(record);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        final ByteBuffer value = ByteBuffer.wrap(body);
        Record record = Record.of(key, value);
        dao.upsert(record);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));

        final Iterator<Record> range =
            dao.rangeWithoutTombstoneFiltering(key, DAO.nextKey(key));

        if (range.hasNext()) {
            final Record first = range.next();
            Record.Value value = first.getValue();

            Response response = value.isTombstone() ?
                new Response(Response.OK, Response.EMPTY) :
                new Response(Response.OK, extractBytes(value.get()));

            response.addHeader("timestamp" + value.timestamp());
            response.addHeader("tombstone" + value.isTombstone());

            return response;
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private List<String> forKeyNodes(String id, int replicas) {
        List<String> list = new LinkedList<>(topology);
        list.sort(Comparator.comparingInt(option -> Hash.murmur3(option + id)));
        return list.stream().limit(replicas).collect(Collectors.toList());
    }

    private byte[] toBytes(String text) {
        return Utf8.toBytes(text);
    }

    private void execute(String id, String replicas, Request request, HttpSession session, Task task) {
        executor.execute(() -> {

            if (checkAndCalcRequestIfLocal(id, request, session, task)) {
                return;
            }

            int ask;
            int from;
            if (replicas == null) {
                ask = topology.size() / 2 + 1;
                from = topology.size();
            } else {
                String[] askFrom = replicas.split("/");
                ask = Integer.parseInt(askFrom[0]);
                from = Integer.parseInt(askFrom[1]);
            }

            if (ask == 0 || ask > from) {
                try {
                    session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                } catch (Exception e) {
                    LOG.error("bad ask = {} or from = {}", ask, from);
                }
                return;
            }

            List<String> nodes = forKeyNodes(id, from);

            List<Response> responses = new ArrayList<>();
            Response res;
            for (String node : nodes) {
                try {
                    HttpClient client = ((MyThread) Thread.currentThread()).clients.get(node);
                    if (client == null) {
                        res = task.call();
                    } else {
                        res = client.invoke(request, TIMEOUT);
                    }
                    responses.add(res);
                } catch (Exception e) {
                    LOG.error("Unexpected exception during method call {}", request.getMethodName(), e);
                    responses.add(new Response(Response.INTERNAL_ERROR, toBytes("Something wrong")));
                }
            }

            calcRequest(request, session, ask, responses);
        });
    }

    private void calcRequest(Request request, HttpSession session, int ask, List<Response> responses) {
        int countSuccess = 0;
        int error = 0;
        long youngest = Long.MIN_VALUE;
        Response result = new Response(Response.NOT_FOUND, Response.EMPTY);

        for (Response response : responses) {

            if (response.getStatus() != getIntSuccessStatus(request)) {
                if (response.getStatus() == 404) {
                    countSuccess++;
                    error++;
                }
                continue;
            }

            countSuccess++;
            if (request.getMethod() != Request.METHOD_GET) {
                continue;
            }

            long respTime = Long.parseLong(response.getHeader("timestamp"));
            if (respTime > youngest) {
                youngest = respTime;
                result = response;
            }
        }

        if (request.getMethod() == Request.METHOD_GET &&
            countSuccess == 0) {
            result = new Response(Response.NOT_FOUND, Response.EMPTY);

        } else if (countSuccess >= ask) {

            if (request.getMethod() == Request.METHOD_GET) {
                boolean isTombstone = Boolean.parseBoolean(result.getHeader("tombstone"));
                if (isTombstone || countSuccess == error) {
                    result = new Response(Response.NOT_FOUND, Response.EMPTY);
                }
            } else {
                result = new Response(getStringSuccessStatus(request), Response.EMPTY);
            }

        } else {
            result = new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        sendResponse(session, result);
    }

    private boolean checkAndCalcRequestIfLocal(String id, Request request, HttpSession session, Task task) {
        if (id.isBlank()) {
            sendResponse(session, new Response(Response.BAD_REQUEST, toBytes("Bad id")));
            return true;
        }

        if (request.getHeader("local") != null) {
            Response call;

            try {
                call = task.call();
            } catch (Exception e) {
                LOG.error("Unexpected exception during method call {}", request.getMethodName(), e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, toBytes("Something wrong")));
                return true;
            }

            sendResponse(session, call);
            return true;
        }
        request.addHeader("local");
        return false;
    }

    private int getIntSuccessStatus(Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return 200; // Response.OK
            case Request.METHOD_PUT:
                return 201; // Response.CREATED
            case Request.METHOD_DELETE:
                return 202; // Response.ACCEPTED
            default:
                return 400; // Response.BAD_REQUEST
        }
    }

    private String getStringSuccessStatus(Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return Response.OK;
            case Request.METHOD_PUT:
                return Response.CREATED;
            case Request.METHOD_DELETE:
                return Response.ACCEPTED;
            default:
                return Response.BAD_REQUEST;
        }
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (Exception e) {
            LOG.error("Can't send response", e);
        }
    }

    private static class MyThread extends Thread {
        public final Map<String, HttpClient> clients;

        public MyThread(Runnable target, int port, Set<String> topology) {
            super(target);
            try {
                clients = extractSelfFromInterface(port, topology);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @FunctionalInterface
    private interface Task {
        Response call();
    }
}
