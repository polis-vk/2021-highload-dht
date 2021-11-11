package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

public abstract class DefaultRequestHandler extends RoutableRequestHandler {

    private static final DTO METHOD_NOT_ALLOWED = DTO.answer(Response.METHOD_NOT_ALLOWED, null);

    public DefaultRequestHandler(
            Cluster.ReplicasHolder replicasHolder, Cluster.Node self, HashRouter<Cluster.Node> router
    ) {
        super(replicasHolder, self, router);
    }

    @Override
    public DTO get(Request request) {
        return METHOD_NOT_ALLOWED;
    }

    @Override
    public DTO put(Request request) {
        return METHOD_NOT_ALLOWED;
    }

    @Override
    public DTO delete(Request request) {
        return METHOD_NOT_ALLOWED;
    }

    @Override
    public DTO other(Request request) {
        return METHOD_NOT_ALLOWED;
    }
}
