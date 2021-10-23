package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class RoutingRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RoutingRequestHandler.class);

    private final Cluster.Node self;
    private final HashRouter<Cluster.Node> router;

    public RoutingRequestHandler(Cluster.Node self, HashRouter<Cluster.Node> router) {
        this.self = self;
        this.router = router;
    }

    @Nullable
    protected abstract String getRouteKey(Request request);

    /**
     * Redirects the request to the host specified by the router.
     *
     * @param request request to redirect
     * @return null if you need to handle the request by yourself, otherwise redirected response
     */
    public final Response checkAndRedirect(Request request) {
        String key = getRouteKey(request);
        if (key == null) {
            return null;
        }

        Cluster.Node target = router.route(key);
        if (target == self) {
            return null;
        }

        return redirect(target, request);
    }

    private Response redirect(Cluster.Node target, Request request) {
        try {
            return target.httpClient.invoke(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Proxy error", e);
            return new Response(Response.INTERNAL_ERROR, "Proxy error".getBytes(StandardCharsets.UTF_8));
        } catch (PoolException | IOException | HttpException e) {
            LOG.error("Proxy error", e);
            return new Response(Response.INTERNAL_ERROR, "Proxy error".getBytes(StandardCharsets.UTF_8));
        }
    }

    protected final byte[] extractBytes(ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
