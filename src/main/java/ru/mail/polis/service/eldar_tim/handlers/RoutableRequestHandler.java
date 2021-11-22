package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class RoutableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RoutableRequestHandler.class);

    protected final Cluster.Node self;
    private final HashRouter<Cluster.Node> router;

    public RoutableRequestHandler(Cluster.Node self, HashRouter<Cluster.Node> router) {
        this.self = self;
        this.router = router;
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
        final Response response;
        if (targetNode == self) {
            response = handleRequest(request).transform();
        } else {
            response = redirectRequest(request, targetNode);
        }
        session.sendResponse(response);
    }

    /**
     * Redirects the request to the specified host.
     *
     * @param request request to redirect
     * @param target target node to redirect request
     * @return response
     */
    protected final Response redirectRequest(Request request, Cluster.Node target) {
        Response response;
        try {
            response = target.getClient().invoke(request);
        } catch (InterruptedException e) {
            String errorText = "Proxy error: interrupted";
            LOG.debug(errorText, e);
            response = new Response(Response.INTERNAL_ERROR, errorText.getBytes(StandardCharsets.UTF_8));
            Thread.currentThread().interrupt();
        } catch (PoolException | IOException | HttpException e) {
            String errorText = "Proxy error: " + e.getMessage();
            LOG.debug(errorText, e);
            response = new Response(Response.INTERNAL_ERROR, errorText.getBytes(StandardCharsets.UTF_8));
        }
        return response;
     }

    protected final byte[] extractBytes(ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
