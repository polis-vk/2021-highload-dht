package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.service.eldar_tim.ServiceResponse;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.service.exceptions.ServerRuntimeException;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public abstract class ReplicableRequestHandler extends RoutableRequestHandler implements RequestHandler {

    public static final String HEADER_HANDLE_LOCALLY = "Service-Handle-Locally";
    public static final String HEADER_HANDLE_LOCALLY_TRUE = HEADER_HANDLE_LOCALLY + ": 1";
    public static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private final Cluster.ReplicasHolder replicasHolder;

    public ReplicableRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder, HttpClient httpClient, Executor workers
    ) {
        super(self, router, httpClient, workers);
        this.replicasHolder = replicasHolder;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (request.getHeader(HEADER_HANDLE_LOCALLY) != null) {
            session.sendResponse(handleLocally(request).timestamped());
            return;
        }

        int[] ackFrom = parseAckFromParameter(request.getParameter("replicas="));
        int ack = ackFrom[0];
        int from = ackFrom[1];

        Cluster.Node targetNode = getTargetNode(request);
        List<Cluster.Node> replicas = replicasHolder.getBunch(targetNode, from);

        ServiceResponse serviceResponse = mergePollResults(makePoll(ack, replicas, request), ack);
        session.sendResponse(serviceResponse.raw());
    }

    private int[] parseAckFromParameter(@Nullable String param) {
        int ack;
        int from;
        int maxFrom = replicasHolder.replicasCount;

        if (param != null) {
            try {
                int indexOf = param.indexOf('/');
                ack = Integer.parseInt(param.substring(0, indexOf));
                from = Integer.parseInt(param.substring(indexOf + 1));
            } catch (IndexOutOfBoundsException | NumberFormatException e) {
                throw new ClientBadRequestException(e);
            }
        } else {
            from = maxFrom;
            ack = quorum(from);
        }

        if (ack < 1 || from < 1 || from > maxFrom || ack > from) {
            throw new ClientBadRequestException(null);
        } else {
            return new int[]{ack, from};
        }
    }

    private int quorum(int from) {
        return from / 2 + 1;
    }

    @Nonnull
    private ServiceResponse handleLocally(@Nonnull Request request) {
        try {
            return handleRequest(request);
        } catch (ServerRuntimeException e) {
            Response answer = new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
            return ServiceResponse.of(answer);
        }
    }

    @Nonnull
    private CompletableFuture<ServiceResponse> handleLocallyAsync(
            @Nonnull Request request, @Nonnull Executor executor
    ) {
        return CompletableFuture.supplyAsync(() -> handleLocally(request), executor);
    }

    @Nonnull
    private CompletableFuture<ServiceResponse> handleRemotelyAsync(
            @Nonnull Cluster.Node target, @Nonnull Request request, @Nonnull Executor executor
    ) {
        return redirectRequestAsync(request, target, executor);
    }

    /**
     * Sends requests to the specified nodes and returns the <code>ack</code> fastest responses,
     * or < <code>ack</code>, if requests failed.
     *
     * @param ack      number of answers
     * @param replicas target nodes for poll
     * @param request  original request
     * @return see the description
     */
    private Collection<ServiceResponse> makePoll(int ack, List<Cluster.Node> replicas, Request request) {
        request.addHeader(HEADER_HANDLE_LOCALLY_TRUE);

        Map<Cluster.Node, CompletableFuture<ServiceResponse>> futures = new HashMap<>(replicas.size());
        for (var target : replicas) {
            if (target == self) {
                futures.put(target, handleLocallyAsync(request, workers));
            } else {
                futures.put(target, handleRemotelyAsync(target, request, workers));
            }
        }

        SortedMap<Cluster.Node, ServiceResponse> results = new TreeMap<>(Comparator.comparing(Cluster.Node::getKey));
        while (true) {
            Iterator<Cluster.Node> iterator = futures.keySet().iterator();
            while (iterator.hasNext()) {
                Cluster.Node node = iterator.next();
                CompletableFuture<ServiceResponse> future = futures.get(node);

                if (future.isDone()) {
                    iterator.remove();
                } else {
                    continue;
                }

                final ServiceResponse response;
                try {
                    response = future.get();
                } catch (ExecutionException | InterruptedException ignored) {
                    continue;
                }

                if (response != null && isCorrect(response)) {
                    results.put(node, response);
                }
            }

            if (results.size() >= ack || futures.isEmpty()) {
                return results.values();
            }

            // Here we might wait others and make repair if needed.
        }
    }

    private boolean isCorrect(@Nonnull ServiceResponse serviceResponse) {
        int status = serviceResponse.raw().getStatus();
        return status < 500;
    }

    /**
     * Combines multiple responses into one according to the latest time criterion.
     * If none of the answers are relevant, the first in the list will be returned.
     *
     * @param responses ordered responses to merge
     * @param ack       minimal success answers count
     * @return one merged response
     */
    private ServiceResponse mergePollResults(Collection<ServiceResponse> responses, int ack) {
        if (responses.size() < ack) {
            return ServiceResponse.of(new Response(RESPONSE_NOT_ENOUGH_REPLICAS, Response.EMPTY));
        } else {
            return Collections.max(responses, Comparator.comparingLong(o -> o.timestamp));
        }
    }
}
