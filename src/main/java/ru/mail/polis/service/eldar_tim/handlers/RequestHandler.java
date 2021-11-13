package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;

public abstract class RequestHandler extends ReplicableRequestHandler {

    private static final ServiceResponse METHOD_NOT_ALLOWED =
            ServiceResponse.of(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));

    public RequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router, Cluster.ReplicasHolder replicasHolder
    ) {
        super(self, router, replicasHolder);
    }

    @Nonnull
    @Override
    protected ServiceResponse handleReplicableRequest(Request request) {
        return METHOD_NOT_ALLOWED;
    }
}
