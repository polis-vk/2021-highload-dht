package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProxyResponse {
    public static final String TOMBSTONE_HEADER = "Tombstone";
    public static final String PROXY_HEADER = "Proxy";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyResponse.class);

    private final ThreadPoolExecutor proxyExecutor;
    private String[] topology;
    private HttpClient[] clients;
    private int id;

    public ProxyResponse(int corePoolSize,
                         int maxPoolSize,
                         int keepAliveTime,
                         TimeUnit timeUnit,
                         Set<String> topology,
                         int me) {
        this.proxyExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                timeUnit,
                new LinkedBlockingQueue<>());
        this.topology = new String[topology.size()];
        this.clients = new HttpClient[topology.size()];

        String[] t = topology.toArray(new String[0]);
        Arrays.sort(t);
        for (int i = 0; i < t.length; i++) {
            String adr = t[i];
            ConnectionString conn = new ConnectionString(adr + "?timeout=100");
            this.topology[i] = adr;
            if (conn.getPort() == me) {
                this.id = i;
                this.clients[i] = null;
                continue;
            }

            this.clients[i] = new HttpClient(conn);
        }
    }

    //assume there is a timestamp here, or it is 404 response with no data
    public static long parseTimeStamp(Response response) {
        byte[] body = response.getBody();
        long result = -1;
        if (body.length >= Long.BYTES) {
            try {
                ByteBuffer wrap = ByteBuffer.wrap(body, body.length - Long.BYTES, Long.BYTES);
                result = wrap.getLong();
            } catch (Exception e) {
                LOGGER.error("Error while parsing timestamp. {}", response, e);
            }
        }
        return result;
    }

    public static boolean isConfirm(Response response) {
        int status = response.getStatus();
        return (status == 200) || (status == 201) || (status == 202) || (status == 404);
    }

    public static Response mergeResponses(Request request, List<Response> responses) {
        final Response result;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                result = mergeGetResponses(responses);
                break;
            case Request.METHOD_PUT:
                result = UtilResponses.createdResponse();
                break;
            case Request.METHOD_DELETE:
                result = UtilResponses.acceptedResponse();
                break;
            default:
                result = UtilResponses.badRequest();
                break;
        }
        return result;
    }

    private static Response mergeGetResponses(List<Response> responses) {
        Response tmp = null;
        long timeStamp = -2;

        for (Response response : responses) {
            long l = ProxyResponse.parseTimeStamp(response);
            if (l > timeStamp) {
                timeStamp = l;
                tmp = response;
            }
        }
        if (timeStamp < 0) {
            return UtilResponses.notFoundResponse();
        }

        Response result;
        try {
            if (tmp.getHeader(TOMBSTONE_HEADER) == null) {
                byte[] body = tmp.getBody();
                byte[] newBody = new byte[body.length - Long.BYTES];
                System.arraycopy(body, 0, newBody, 0, newBody.length);

                result = new Response(Integer.toString(tmp.getStatus()), newBody);
            } else {
                result = UtilResponses.notFoundResponse();
            }
        } catch (Exception e) {
            LOGGER.error("Error in merging responses {}", responses, e);
            result = UtilResponses.serviceUnavailableResponse();
        }

        return result;
    }

    public synchronized void shutdown() {
        proxyExecutor.shutdown();
        try {
            if (!proxyExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await termination on close");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Error on shutdown", e);
            Thread.currentThread().interrupt();
        }

        if (clients != null) {
            for (HttpClient client : clients) {
                if (client != null) {
                    client.clear();
                }
            }
        }

        clients = null;
        topology = null;
    }

    public List<Integer> getNodeId(String id) {
        if (id.isBlank()) {
            return new ArrayList<>();
        }

        PriorityQueue<Pair<Integer, Integer>> topologyIds = new PriorityQueue<>(Comparator.comparing(a -> a.first));
        for (int i = 0; i < topology.length; i++) {
            int hashCode = (topology[i] + id).hashCode();
            topologyIds.add(new Pair<>(hashCode, i));
        }

        ArrayList<Integer> list = new ArrayList<>(topologyIds.size());
        while (topologyIds.peek() != null) {
            list.add(topologyIds.poll().second);
        }

        return list;
    }

    public int size() {
        return topology.length;
    }

    public boolean isMe(Integer id) {
        return id == this.id;
    }

    public Response invoke(Integer httpClientId, Request request) {
        try {
            return clients[httpClientId].invoke(request);
        } catch (Exception e) {
            LOGGER.error("Something wrong on request httpClient {}, request: {}, topology: {}",
                    clients[httpClientId], request, topology, e);
            return UtilResponses.serviceUnavailableResponse();
        }
    }
}
