package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Cluster;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public abstract class ReplicableRequestHandler extends RoutableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicableRequestHandler.class);

    private static final String HEADER_HANDLE_LOCALLY = "Service-Handle-Locally";
    private static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private final Cluster.ReplicasHolder replicasHolder;

    public ReplicableRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder
    ) {
        super(self, router);
        this.replicasHolder = replicasHolder;
    }

    @Nonnull
    protected abstract ServiceResponse handleReplicableRequest(Request request);

    @Override
    public final void handleRequest(Request request, HttpSession session) throws IOException {
        session.sendResponse(handleLocally(request));
    }

    public void handleReplicableRequest(Cluster.Node target, Request request, HttpSession session) throws IOException {
        if (request.getHeader(HEADER_HANDLE_LOCALLY) != null) {
            session.sendResponse(handleLocally(request));
            return;
        }

        int[] askFrom = parseAskFromParameter(request.getParameter("replicas="));
        int ask = askFrom[0];
        int from = askFrom[1];

        List<Cluster.Node> nodes = replicasHolder.getReplicas(from, target);

        var pollResultsHandler = new ReplicasPollResultsHandler();
        var pollRecursiveTask = new ReplicasPollRecursiveTask(ask, nodes, request, pollResultsHandler);
        Response result = pollRecursiveTask.invoke();

        session.sendResponse(result);
    }

    /**
     * Detects if the current node should parse request.
     * This node should parse the request, if it's included in the list of requested replicas.
     *
     * @param target target node
     * @param request current request
     * @return true, if the current node should parse request, otherwise false
     */
    public boolean shouldHandleLocally(Cluster.Node target, Request request) {
        if (target == self) {
            return true;
        }

        int[] askFrom;
        try {
            String param = request.getParameter("replicas=");
            askFrom = parseAskFromParameter(param);
        } catch (ClientBadRequestException e) {
            return true;
        }

        return replicasHolder.getReplicas(askFrom[1], target).contains(self);
    }

    private int[] parseAskFromParameter(@Nullable String param) {
        int ask, from, maxFrom = replicasHolder.replicasCount + 1;

        if (param != null) {
            try {
                int indexOf = param.indexOf('/');
                ask = Integer.parseInt(param.substring(0, indexOf));
                from = Integer.parseInt(param.substring(indexOf + 1));
            } catch (IndexOutOfBoundsException | NumberFormatException e) {
                throw new ClientBadRequestException(e);
            }
        } else {
            from = maxFrom;
            ask = quorum(from);
        }

        if (ask < 1 || from < 1 || from > maxFrom || ask > from) {
            throw new ClientBadRequestException(null);
        } else {
            return new int[]{ask, from};
        }
    }

    private int quorum(int from) {
        return (int) Math.ceil(from * 0.51);
    }

    private Response handleLocally(@Nonnull Request request) {
        return handleReplicableRequest(request).transform();
    }

    private Response handleRemote(@Nonnull Request request, @Nonnull HttpClient client) {
        try {
            return client.invoke(request);
        } catch (InterruptedException e) {
            String errorText = "Proxy error: interrupted";
            LOG.debug(errorText, e);
            Thread.currentThread().interrupt();
        } catch (PoolException | IOException | HttpException e) {
            String errorText = "Proxy error: " + e.getMessage();
            LOG.debug(errorText, e);
        }
        return null;
    }

    private final class ReplicasPollRecursiveTask extends RecursiveTask<Response> {

        private final boolean master;

        private final int ask;
        private final List<Cluster.Node> from;
        private final Request request;
        private final ReplicasPollResultsHandler handler;

        public ReplicasPollRecursiveTask(int ask, List<Cluster.Node> from, Request request, ReplicasPollResultsHandler handler) {
            this.master = true;
            this.ask = ask;
            this.from = from;
            this.request = request;
            this.handler = handler;
        }

        private ReplicasPollRecursiveTask(Cluster.Node from, Request request) {
            this.master = false;
            this.ask = 1;
            this.from = Collections.singletonList(from);
            this.request = request;
            this.handler = null;
        }

        @Override
        protected Response compute() {
            if (master) {
                return master();
            } else {
                return slave();
            }
        }

        private Response master() {
            request.addHeader(HEADER_HANDLE_LOCALLY + ": 1");

            List<ForkJoinTask<Response>> futures = new ArrayList<>(from.size());
            for (Cluster.Node node : from) {
                var task = new ReplicasPollRecursiveTask(node, request);
                futures.add(task);
            }
            futures.forEach(ForkJoinTask::fork);

            List<Response> results = new ArrayList<>();
            while (true) {
                ListIterator<ForkJoinTask<Response>> iterator = futures.listIterator();
                while (iterator.hasNext()) {
                    ForkJoinTask<Response> future = iterator.next();
                    if (!future.isDone()) {
                        continue;
                    } else {
                        iterator.remove();
                    }

                    Response response = future.join();
                    if (response != null && isCorrect(response)) {
                        results.add(response);
                    } else {
                        LOG.debug("Got incorrect answer from replica: " + response);
                    }
                }

                if (results.size() >= ask || futures.isEmpty()) {
                    return handler.handle(request, results, ask);
                }

                // to do: wait others and make repair if needed
            }
        }

        private Response slave() {
            Cluster.Node target = from.get(0);
            if (target == self) {
                return handleLocally(request);
            } else {
                return handleRemote(request, target.httpClient);
            }
        }

        private boolean isCorrect(@Nonnull Response response) {
            int status = response.getStatus();
            return status < 500;
        }
    }

    private static final class ReplicasPollResultsHandler {

        public ReplicasPollResultsHandler() {
            // No need.
        }

        public Response handle(Request request, List<Response> responses, int ask) {
            int answers = responses.size();
            if (answers < ask) {
                return new Response(RESPONSE_NOT_ENOUGH_REPLICAS, Response.EMPTY);
            }

            if (request.getMethod() == Request.METHOD_GET) {
                return findMostRecentData(responses);
            } else {
                return responses.get(0);
            }
        }

        private Response findMostRecentData(List<Response> responses) {
            Response mostRecentResponse = responses.get(0);
            long mostRecentTimestamp = -1;

            for (Response response : responses) {
                String timestampHeader = response.getHeader(ServiceResponse.HEADER_TIMESTAMP);
                if (timestampHeader == null) {
                    continue;
                }

                long timestamp;
                try {
                    timestamp = Long.parseLong(timestampHeader);
                } catch (NumberFormatException e) {
                    timestamp = -1;
                }
                if (timestamp > mostRecentTimestamp) {
                    mostRecentResponse = response;
                    mostRecentTimestamp = timestamp;
                }
            }
            return mostRecentResponse;
        }
    }
}
