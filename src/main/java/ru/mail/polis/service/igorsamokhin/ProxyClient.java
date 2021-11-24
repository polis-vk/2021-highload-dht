package ru.mail.polis.service.igorsamokhin;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyClient.class);

    private final String[] topology;
    private final ExecutorService executor;
    private final HttpClient client;

    private int id;

    public ProxyClient(int corePoolSize,
                       Set<String> topology,
                       int me) throws URISyntaxException {
        this.topology = new String[topology.size()];
        this.executor = Executors.newFixedThreadPool(corePoolSize);

        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .executor(executor)
                .build();

        String[] t = topology.toArray(new String[0]);
        Arrays.sort(t);
        for (int i = 0; i < t.length; i++) {
            String adr = t[i];
            this.topology[i] = adr;

            URI uri = new URI(adr);
            if (uri.getPort() == me) {
                this.id = i;
            }
        }
    }

    public Response proxy(Request request, List<Integer> ids, int ack, int from,
                          Callable<Response> thisNodeHandler) {
        request.addHeader(Utils.PROXY_HEADER_ONE_NIO);

        CopyOnWriteArrayList<Response> responses = new CopyOnWriteArrayList<>(new ArrayList<>(from));
        AtomicInteger confirms = new AtomicInteger(0);
        AtomicInteger allResponses = new AtomicInteger(0);

        CompletableFuture<Boolean> doneFlag = new CompletableFuture<>();
        for (int i = 0; i < from; i++) {
            ResponseFuture ultraResponse = askHttpClient(request, ids.get(i), thisNodeHandler);
            ultraResponse.thenCompose(r -> {
                responses.add(r);
                int all = allResponses.incrementAndGet();

                if (doneFlag.isDone()) {
                    return;
                }

                int currentConfirms = isConfirm(r) ? confirms.incrementAndGet() : confirms.get();
                if (currentConfirms >= ack) {
                    doneFlag.complete(true);
                } else if (all >= from) {
                    doneFlag.complete(false);
                }
            });
        }

        Response result;
        try {
            if (Boolean.TRUE.equals(doneFlag.get(2, TimeUnit.SECONDS))) {
                result = mergeSuccessfulResponses(request, responses);
            } else {
                result = Utils.responseWithMessage("504", "Not Enough Replicas");
            }
        } catch (Exception e) {
            LOGGER.info("Something wrong on waiting proxied responses", e);
            result = Utils.serviceUnavailableResponse();
        }
        return result;
    }

    private ResponseFuture askHttpClient(Request request,
                                         Integer httpClientId,
                                         Callable<Response> thisNodeHandler) {
        ResponseFuture response;
        try {
            if (isMe(httpClientId)) {
                response = new ResponseFuture(thisNodeHandler.call(), executor);
            } else {
                response = sendAsync(httpClientId, request);
            }
        } catch (Exception e) {
            LOGGER.error("Error in askHttpClient request:\n{} httpClientId: {}", request, httpClientId, e);
            response = new ResponseFuture(Utils.serviceUnavailableResponse(), executor);
        }

        return response;
    }

    //assume that response always contains timestamp, or 404 status with no data
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

    public static Response mergeSuccessfulResponses(Request request, List<Response> responses) {
        final Response result;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                result = mergeGetResponses(responses);
                break;
            case Request.METHOD_PUT:
                result = Utils.createdResponse();
                break;
            case Request.METHOD_DELETE:
                result = Utils.acceptedResponse();
                break;
            default:
                result = Utils.badRequest();
                break;
        }
        return result;
    }

    private static Response mergeGetResponses(List<Response> responses) {
        Response tmp = Utils.emptyResponse("");
        long timeStamp = -2;

        for (Response response : responses) {
            if (response == null) {
                continue;
            }

            long l = ProxyClient.parseTimeStamp(response);
            if (l > timeStamp) {
                timeStamp = l;
                tmp = response;
            }
        }
        if (timeStamp < 0) {
            return Utils.notFoundResponse();
        }

        Response result;
        try {
            if (tmp.getHeader(Utils.TOMBSTONE_HEADER_ONE_NIO) == null) {
                byte[] body = tmp.getBody();
                byte[] newBody = new byte[body.length - Long.BYTES];
                System.arraycopy(body, 0, newBody, 0, newBody.length);

                result = new Response(Integer.toString(tmp.getStatus()), newBody);
            } else {
                result = Utils.notFoundResponse();
            }
        } catch (Exception e) {
            LOGGER.error("Error in merging responses {}", responses, e);
            result = Utils.serviceUnavailableResponse();
        }

        return result;
    }

    public synchronized void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Can't await termination on close");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Error on shutdown", e);
            Thread.currentThread().interrupt();
        }
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

    private ResponseFuture sendAsync(Integer builderId, Request request) {
        HttpRequest newRequest = requestFrom(request, topology[builderId] + request.getURI());

        ResponseFuture response;
        try {
            CompletableFuture<Void> startLever = new CompletableFuture<>();
            CompletableFuture<HttpResponse<byte[]>> end = startLever
                    .thenCompose(v -> client.sendAsync(newRequest, HttpResponse.BodyHandlers.ofByteArray()));
            response = new ResponseFuture(end, startLever, executor);
        } catch (Exception e) {
            LOGGER.error("Something wrong on request httpClient {}, \noldRequest: {} \nnewRequest: {}\n topology: {}",
                    builderId, request, newRequest, topology, e);
            response = new ResponseFuture(Utils.serviceUnavailableResponse(), executor);
        }
        return response;
    }

    private HttpRequest requestFrom(Request request, String uri) {
        byte[] body = request.getBody();
        HttpRequest.BodyPublisher bodyPublisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .method(request.getMethodName(), bodyPublisher)
                .uri(URI.create(uri))
                .timeout(Duration.ofSeconds(1));

        if (request.getHeader(Utils.PROXY_HEADER_ONE_NIO) != null) {
            builder.setHeader(Utils.PROXY_HEADER, "");
        }

        return builder.build();
    }
}
