package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class RoutingRequestHandler implements RequestHandler {

    private final Cluster.Node self;
    private final HashRouter<Cluster.Node> router;

    public RoutingRequestHandler(Cluster.Node self, HashRouter<Cluster.Node> router) {
        this.self = self;
        this.router = router;
    }

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
        HttpClient client = new HttpClient(new ConnectionString("http://" + target.ip + ":" + target.port));
        try {
            final Response response;
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    response = client.get(request.getURI(), request.getHeaders());
                    break;
                case Request.METHOD_PUT:
                    response = client.put(request.getURI(), request.getBody(), request.getHeaders());
                    break;
                case Request.METHOD_DELETE:
                    response = client.delete(request.getURI(), request.getHeaders());
                    break;
                default:
                    response = new Response(Response.BAD_REQUEST,
                            "This method unsupported by proxy".getBytes(StandardCharsets.UTF_8));
            }
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
        } catch (PoolException | IOException | HttpException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Nullable
    protected abstract String getRouteKey(Request request);
}
