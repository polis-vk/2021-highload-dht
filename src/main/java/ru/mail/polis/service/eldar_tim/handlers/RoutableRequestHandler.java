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
import ru.mail.polis.service.eldar_tim.ServiceResponseBodySubscriber;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public abstract class RoutableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RoutableRequestHandler.class);

    protected final Cluster.Node self;
    private final HashRouter<Cluster.Node> router;
    protected final HttpClient httpClient;
    protected final Executor workers;

    public RoutableRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            HttpClient httpClient, Executor workers) {
        this.self = self;
        this.router = router;
        this.httpClient = httpClient;
        this.workers = workers;
    }

    /**
     * Defines the key for detecting the owner node.
     *
     * @param request target request
     * @return string representation of the key
     */
    @Nullable
    protected abstract String getRouteKey(Request request);

    /**
     * Detects the node to redirect request.
     *
     * @param request request to redirect
     * @return node to redirect request
     */
    @Nonnull
    protected final Cluster.Node getTargetNode(Request request) {
        String key = getRouteKey(request);
        if (key == null) {
            return self;
        }

        return router.route(key);
    }

    @Nonnull
    protected abstract ServiceResponse handleRequest(Request request);

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        Cluster.Node targetNode = getTargetNode(request);
        final ServiceResponse response;
        if (targetNode == self) {
            response = handleRequest(request);
        } else {
            response = redirectRequest(request, targetNode);
        }
        session.sendResponse(response.raw());
    }

    /**
     * Synchronously redirects the request to the specified host.
     *
     * @param request request to redirect
     * @param target  target node to redirect request
     * @return response
     */
    protected final ServiceResponse redirectRequest(Request request, Cluster.Node target) {
        try {
            return redirectRequestAsync(request, target, workers).get();
        } catch (CancellationException | ExecutionException e) {
            String errorText = "Proxy error: " + e.getMessage();
            LOG.debug(errorText, e);

            Response answer = new Response(Response.INTERNAL_ERROR, errorText.getBytes(StandardCharsets.UTF_8));
            return ServiceResponse.of(answer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String errorText = "Proxy error: interrupted";
            LOG.debug(errorText, e);

            Response answer = new Response(Response.INTERNAL_ERROR, errorText.getBytes(StandardCharsets.UTF_8));
            return ServiceResponse.of(answer);
        }
    }

    /**
     * Asynchronously redirects the request to the specified host.
     *
     * @param request request to redirect
     * @param target target node to redirect request
     * @param workers executor for response post-processing
     * @return response
     */
    protected final CompletableFuture<ServiceResponse> redirectRequestAsync(
            Request request, Cluster.Node target, Executor workers
    ) {
        HttpRequest mappedRequest = HttpUtils.mapRequest(request, target);
        return httpClient
                .sendAsync(mappedRequest, ServiceResponseBodySubscriber.INSTANCE)
                .thenApply(HttpResponse::body);
    }

    protected final byte[] extractBytes(ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
