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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.BiFunction;

public abstract class ReplicableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicableRequestHandler.class);

    private static final String HEADER_NOT_USE_REPLICAS = "Not-Use-Replicas: 1";
    private static final String RESPONSE_NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private final Cluster.ReplicasHolder replicasHolder;
    public final Cluster.Node self;

    public ReplicableRequestHandler(Cluster.ReplicasHolder replicasHolder, Cluster.Node self) {
        this.replicasHolder = replicasHolder;
        this.self = self;
    }

    public abstract DTO get(Request request);

    public abstract DTO put(Request request);

    public abstract DTO delete(Request request);

    public abstract DTO other(Request request);

    public void handleRequest(Request request, HttpSession session, Cluster.Node target) throws IOException {
        if (request.getHeader(HEADER_NOT_USE_REPLICAS) != null) {
            writeDTO(handleLocally(request), session);
            return;
        }

        int[] askFrom = parseAskFromParameter(request.getParameter("replicas="));
        int ask = askFrom[0];
        int from = askFrom[1];

        List<Cluster.Node> nodes = replicasHolder.getReplicas(from - 1, target);
        if (self != target) {
            nodes.add(0, target);
            nodes.remove(self);
        }

        request.addHeader(HEADER_NOT_USE_REPLICAS);
        List<ForkJoinTask<DTO>> forks = fork(nodes, request);

        // to do: calculate access time for each host
        // to do: make non-blocking HttpClient

        List<DTO> answers = new ArrayList<>(ask);
        ListIterator<ForkJoinTask<DTO>> iterator = forks.listIterator();
        while (iterator.hasNext() && answers.size() < ask) {
            DTO answer = iterator.next().join();
            if (answer != null && answer.isOk) {
                answers.add(answer);
            }
            iterator.remove();
        }

        parseResults(ask, answers, session);

        // to do: make synchronization (repair)

        while (iterator.hasNext()) {
            iterator.next().quietlyJoin();
        }
    }

    private void parseResults(int ask, List<DTO> answers, HttpSession session) throws IOException {
        if (answers.size() < ask) {
            session.sendError(RESPONSE_NOT_ENOUGH_REPLICAS, RESPONSE_NOT_ENOUGH_REPLICAS);
            return;
        }

        if (dto.isOk) {
            byte[] value = dto.value != null ? dto.value : Response.EMPTY;
            session.sendResponse(new Response(Response.OK, value));
        } else {
            session.sendError(dto.responseCode, dto.errorMessage);
        }
    }

    private List<ForkJoinTask<DTO>> fork(List<Cluster.Node> nodes, Request request) {
        List<ForkJoinTask<DTO>> tasks = new ArrayList<>(nodes.size() + 1);
        for (Cluster.Node node : nodes) {
            AsyncRequestTask task = new AsyncRequestTask(request, node.httpClient, this::handleRemote);
            tasks.add(task.fork());
        }
        AsyncRequestTask selfTask = new AsyncRequestTask(request, null, (r, c) -> handleLocally(r));
        tasks.add(0, selfTask.fork());
        return tasks;
    }

    /**
     * Detects if the current node should parse request.
     * This node should parse the request, if it's included in the list of requested replicas.
     *
     * @param request current request
     * @param target target node
     * @return true, if the current node should parse request, otherwise false
     */
    public boolean shouldParse(Request request, Cluster.Node target) {
        if (target == self) {
            return true;
        }

        String param = request.getParameter("replicas=");
        int[] askFrom = parseAskFromParameter(param);

        return replicasHolder.getReplicas(askFrom[1] - 1, target).contains(self);
    }

    private int[] parseAskFromParameter(@Nullable String param) {
        int ask = -1, from = -1;

        if (param != null) {
            try {
                int indexOf = param.indexOf('/');
                ask = Integer.parseInt(param.substring(0, indexOf));
                from = Integer.parseInt(param.substring(indexOf + 1));
            } catch (IndexOutOfBoundsException | NumberFormatException ignored) {
                ask = -1;
            }
        }

        int maxFrom = replicasHolder.replicasCount + 1;
        if (param == null || ask < 1 || from < 1 || from > maxFrom || ask > from) {
            from = maxFrom;
            ask = quorum(from);
        }
        return new int[]{ask, from};
    }

    private int quorum(int from) {
        return (int) Math.ceil(from * 0.5);
    }

    private DTO handleLocally(@Nonnull Request request) {
        final DTO dto;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                dto = get(request);
                break;
            case Request.METHOD_PUT:
                dto = put(request);
                break;
            case Request.METHOD_DELETE:
                dto = delete(request);
                break;
            default:
                dto = other(request);
        }
        return dto;
    }

    private DTO handleRemote(@Nonnull Request request, @Nonnull HttpClient client) {
        try {
            return parseResponse(client.invoke(request));
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

    private DTO parseResponse(Response response) {
        byte[] body = response.getBody();
        if (body != null) {
            try (
                    ByteArrayInputStream is = new ByteArrayInputStream(body);
                    ObjectInputStream objectIs = new ObjectInputStream(is)
            ) {
                return (DTO) objectIs.readObject();
            } catch (IOException | ClassNotFoundException e) {
                LOG.debug("Exception while parsing replica's answer body", e);
            }
        }
        return null;
    }

    private void writeDTO(DTO dto, HttpSession session) throws IOException {
        if (!dto.isOk) {
            session.sendError(dto.responseCode, dto.errorMessage);
            return;
        }

        byte[] result;
        try (
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                ObjectOutputStream objectOs = new ObjectOutputStream(os)
        ) {
            objectOs.writeObject(dto);
            objectOs.flush();
            os.flush();
            result = os.toByteArray();
        } catch (IOException e) {
            LOG.debug("Exception while serializing DTO", e);
            session.sendError(Response.INTERNAL_ERROR, null);
            return;
        }
        session.sendResponse(new Response(dto.responseCode, result));
    }

    private static class AsyncRequestTask extends RecursiveTask<DTO> {
        private final Request request;
        private final HttpClient client;
        private final BiFunction<Request, HttpClient, DTO> function;

        public AsyncRequestTask(
                Request request, HttpClient client, BiFunction<Request, HttpClient, DTO> function
        ) {
            this.request = request;
            this.client = client;
            this.function = function;
        }

        @Override
        protected DTO compute() {
            return function.apply(request, client);
        }
    }
}
