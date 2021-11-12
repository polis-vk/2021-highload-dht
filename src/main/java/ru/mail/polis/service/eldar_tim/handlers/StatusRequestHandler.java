package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;
import ru.mail.polis.sharding.HashRouter;

import javax.annotation.Nullable;
import java.io.IOException;

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

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        Response response = Response.ok(Response.OK);
        session.sendResponse(response);
    }
}
