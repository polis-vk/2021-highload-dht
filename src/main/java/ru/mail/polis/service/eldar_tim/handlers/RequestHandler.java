package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

public abstract class RequestHandler extends ReplicableRequestHandler {

    private static final DTO METHOD_NOT_ALLOWED = DTO.answer(Response.METHOD_NOT_ALLOWED, null);

    public RequestHandler(
            Cluster.Node self, HashRouter<Cluster.Node> router, Cluster.ReplicasHolder replicasHolder
    ) {
        super(self, router, replicasHolder);
    }

    @Override
    protected DTO handleRequest(Request request) {
        return METHOD_NOT_ALLOWED;
    }
}
