package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StatusRequestHandler extends RequestHandler {

    public StatusRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router, Cluster.ReplicasHolder replicasHolder
    ) {
        super(self, router, replicasHolder);
    }

    @Nullable
    @Override
    protected String getRouteKey(Request request) {
        return null;
    }

    @Nonnull
    @Override
    public ServiceResponse handleReplicableRequest(Request request) {
        return ServiceResponse.of(Response.ok(Response.OK));
    }
}
