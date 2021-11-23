package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Cluster;
import ru.mail.polis.service.HttpUtils;
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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ReplicableRequestHandler extends RoutableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicableRequestHandler.class);

    public static final String HEADER_HANDLE_LOCALLY = "Service-Handle-Locally";
    public static final String HEADER_HANDLE_LOCALLY_TRUE = HEADER_HANDLE_LOCALLY + ": 1";
    public static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private final Cluster.ReplicasHolder replicasHolder;

    public ReplicableRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder, HttpClient httpClient, Executor workers, Executor proxies
    ) {
        super(self, router, httpClient, workers, proxies);
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

        pollReplicas(ack, replicas, request)
                .whenCompleteAsync((response, t) -> {
                    if (response != null) {
                        HttpUtils.sendResponse(LOG, session, response.raw());
                    } else {
                        HttpUtils.sendError(LOG, session, Response.INTERNAL_ERROR, t.getMessage());
                    }
                }, proxies);
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
            var answer = new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
            return ServiceResponse.of(answer);
        }
    }

    @Nonnull
    private CompletableFuture<ServiceResponse> handleRemotelyAsync(
            @Nonnull Cluster.Node target, @Nonnull Request request
    ) {
        return redirectRequestAsync(request, target);
    }

    private CompletableFuture<ServiceResponse> pollReplicas(
            int ack, List<Cluster.Node> replicas, Request request
    ) {
        request.addHeader(HEADER_HANDLE_LOCALLY_TRUE);

        PollHandler handler = new PollHandler(ack, replicas.size());
        CompletableFuture<ServiceResponse> localHandler = null;

        for (var target : replicas) {
            final CompletableFuture<ServiceResponse> future;
            if (target == self) {
                future = localHandler = new CompletableFuture<>();
            } else {
                future = handleRemotelyAsync(target, request);
            }
            future.whenComplete((r, t) -> handler.parse(r));
        }

        if (localHandler != null) {
            localHandler.complete(handleLocally(request));
        }

        return handler.result;
    }

    private static final class PollHandler {

        private final int ack;
        private final int from;
        private final CompletableFuture<ServiceResponse> result;

        private final Queue<ServiceResponse> succeedResponses = new ConcurrentLinkedQueue<>();
        private final Queue<ServiceResponse> failedResponses = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean parsingRequest = new AtomicBoolean();

        public PollHandler(int ack, int from) {
            this.ack = ack;
            this.from = from;
            this.result = new CompletableFuture<>();
        }

        public void parse(@Nullable ServiceResponse response) {
            if (response != null && isCorrect(response)) {
                succeedResponses.add(response);
            } else {
                failedResponses.add(response);
            }

            if (requestProcessing()) {
                ServiceResponse merged = mergePollResults(succeedResponses, ack);
                result.complete(merged);
            }
        }

        private boolean requestProcessing() {
            int successSize = succeedResponses.size();
            int totalSize = successSize + failedResponses.size();
            return (successSize >= ack || totalSize >= from)
                    && parsingRequest.compareAndSet(false, true);
        }

        private boolean isCorrect(@Nonnull ServiceResponse serviceResponse) {
            int status = serviceResponse.raw().getStatus();
            return status < 500;
        }

        /**
         * Combines multiple responses into one according to the latest time criterion.
         * If none of the answers are relevant, the first in the list will be returned.
         *
         * @param success ordered responses to merge
         * @param ack     minimal success answers count
         * @return one merged response
         */
        private ServiceResponse mergePollResults(Collection<ServiceResponse> success, int ack) {
            if (success.size() < ack) {
                return ServiceResponse.of(new Response(RESPONSE_NOT_ENOUGH_REPLICAS, Response.EMPTY));
            } else {
                return Collections.max(success, Comparator.comparingLong(o -> o.timestamp));
            }
        }
    }
}
