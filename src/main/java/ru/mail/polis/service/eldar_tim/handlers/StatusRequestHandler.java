package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.http.HttpClient;
import java.util.concurrent.Executor;

public class StatusRequestHandler extends RequestHandler {

    public StatusRequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder, HttpClient httpClient, Executor workers
    ) {
        super(self, router, replicasHolder, httpClient, workers);
    }

    @Nullable
    @Override
    protected String getRouteKey(Request request) {
        return null;
    }

    @Nonnull
    @Override
    public ServiceResponse handleRequest(Request request) {
        return ServiceResponse.of(Response.ok(Response.OK));
    }
}
