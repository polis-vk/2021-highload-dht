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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public final class BasicService extends HttpServer implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(BasicService.class);
    private static final int TIMEOUT = 100; // TODO config
    private final DAO dao;
    private final Executor executor;
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
        Map<String, HttpClient> clients = new HashMap<>();
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
        @Param(value = "id", required = true) final String id
    ) {
        execute(id, request, session, () -> {

            if (id.isBlank()) {
                return new Response(Response.BAD_REQUEST, toBytes("Bad id"));
            }

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

    @Override
    public synchronized void stop() {
        super.stop();
    /*FIXME    for (HttpClient client : clients.values()) {
            client.close();
        }*/
    }

    private Response delete(String id) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        final ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response get(String id) {
        final ByteBuffer key = ByteBuffer.wrap(toBytes(id));
        final Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            final Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        }
        return new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private HttpClient forKey(String id) {
        String bestMatch = null;
        int maxHash = Integer.MIN_VALUE;

        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            if (hash > maxHash) { // it is important to have topology consistently sorted
                bestMatch = option;
                maxHash = hash;
            }
        }

        if (bestMatch == null) {
            throw new IllegalStateException("No nodes?");
        }

        // local node for no client
        return ((MyThread) Thread.currentThread()).clients.get(bestMatch);
    }

    private byte[] toBytes(String text) {
        return Utf8.toBytes(text);
    }

    private void execute(String id, Request request, HttpSession session, Task task) {
        executor.execute(() -> {

            Response call;
            try {
                HttpClient client = forKey(id);
                if (client != null) {
                    call = client.invoke(request, TIMEOUT);
                } else {
                    call = task.call();
                }
            } catch (Exception e) {
                LOG.error("Unexpected exception during method call {}", request.getMethodName(), e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, toBytes("Something wrong")));
                return;
            }

            sendResponse(session, call);
        });
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (Exception e) {
            LOG.error("Can't send response", e);
        }
    }

    private static class MyThread extends Thread {
        final Map<String, HttpClient> clients;

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
