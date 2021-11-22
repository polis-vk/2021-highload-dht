package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.service.exceptions.ClientBadRequestException;
import ru.mail.polis.service.exceptions.ServerRuntimeException;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public abstract class ReplicableRequestHandler extends RoutableRequestHandler implements RequestHandler {

    public static final String HEADER_HANDLE_LOCALLY = "Service-Handle-Locally";
    public static final String HEADER_HANDLE_LOCALLY_TRUE = HEADER_HANDLE_LOCALLY + ": 1";
    public static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private final Cluster.ReplicasHolder replicasHolder;
    private final ReplicasPollResultsHandler pollResultsHandler;

    public ReplicableRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder
    ) {
        super(self, router);
        this.replicasHolder = replicasHolder;
        this.pollResultsHandler = new ReplicasPollResultsHandler();
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (request.getHeader(HEADER_HANDLE_LOCALLY) != null) {
            session.sendResponse(handleLocally(request));
            return;
        }

        int[] askFrom = parseAskFromParameter(request.getParameter("replicas="));
        int ask = askFrom[0];
        int from = askFrom[1];

        Cluster.Node targetNode = getTargetNode(request);
        List<Cluster.Node> replicas = replicasHolder.getBunch(targetNode, from);

        Response response = new ReplicasPollRecursiveTask(ask, replicas, request).invoke();
        session.sendResponse(response);
    }

    private int[] parseAskFromParameter(@Nullable String param) {
        int ask;
        int from;
        int maxFrom = replicasHolder.replicasCount;

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
        return from / 2 + 1;
    }

    @Nonnull
    private Response handleLocally(@Nonnull Request request) {
        try {
            return handleRequest(request).transform();
        } catch (ServerRuntimeException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Nonnull
    private Response handleRemotely(@Nonnull Request request, @Nonnull Cluster.Node target) {
        return redirectRequest(request, target);
    }

    private final class ReplicasPollRecursiveTask extends RecursiveTask<Response> {

        private final boolean master;

        private final int ask;
        private final List<Cluster.Node> from;
        private final Request request;

        public ReplicasPollRecursiveTask(
                int ask, List<Cluster.Node> from, Request request
        ) {
            super();
            this.master = true;
            this.ask = ask;
            this.from = from;
            this.request = request;
        }

        private ReplicasPollRecursiveTask(Cluster.Node from, Request request) {
            super();
            this.master = false;
            this.ask = 1;
            this.from = Collections.singletonList(from);
            this.request = request;
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
            request.addHeader(HEADER_HANDLE_LOCALLY_TRUE);

            List<ForkJoinTask<Response>> futures = new ArrayList<>(from.size());
            for (Cluster.Node node : from) {
                var task = new ReplicasPollRecursiveTask(node, request);
                futures.add(task.fork());
            }

            return parseFutures(futures);
        }

        private Response slave() {
            Cluster.Node target = from.get(0);
            if (target == self) {
                return handleLocally(request);
            } else {
                return handleRemotely(request, target);
            }
        }

        private Response parseFutures(Collection<ForkJoinTask<Response>> futures) {
            List<Response> results = new ArrayList<>();
            while (true) {
                Iterator<ForkJoinTask<Response>> iterator = futures.iterator();
                while (iterator.hasNext()) {
                    ForkJoinTask<Response> future = iterator.next();
                    if (future.isDone() || results.size() < ask) {
                        iterator.remove();
                    } else {
                        continue;
                    }

                    Response response = future.join();
                    if (response != null && isCorrect(response)) {
                        results.add(response);
                    }
                }

                if (results.size() >= ask || futures.isEmpty()) {
                    return pollResultsHandler.handle(results, ask);
                }

                // Here we might wait others and make repair if needed.
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

        public Response handle(List<Response> responses, int ask) {
            int answers = responses.size();
            if (answers < ask) {
                return new Response(RESPONSE_NOT_ENOUGH_REPLICAS, Response.EMPTY);
            } else {
                return findMostRecent(responses);
            }
        }

        private Response findMostRecent(List<Response> responses) {
            Response mostRecentResponse = responses.get(0);
            long mostRecentTimestamp = -1;

            for (Response response : responses) {
                String timestampHeader = response.getHeader(ServiceResponse.HEADER_TIMESTAMP);
                if (timestampHeader == null) {
                    continue;
                }

                long timestamp;
                try {
                    timestamp = Long.parseLong(timestampHeader, 2, timestampHeader.length(), 10);
                } catch (IndexOutOfBoundsException | NumberFormatException e) {
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
