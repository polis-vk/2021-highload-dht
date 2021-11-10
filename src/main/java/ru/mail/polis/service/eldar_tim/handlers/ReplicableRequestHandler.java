package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Cluster;

import javax.annotation.Nullable;
import java.io.IOException;

public abstract class ReplicableRequestHandler implements RequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicableRequestHandler.class);

    private final Cluster.ReplicasManager replicasManager;
    public final Cluster.Node self;

    public ReplicableRequestHandler(Cluster.ReplicasManager replicasManager, Cluster.Node self) {
        this.replicasManager = replicasManager;
        this.self = self;
    }

    /**
     * Detects if the current node should parse request.
     *
     * @param request current request
     * @param target target node
     * @return true, if the current node should parse request, otherwise false
     */
    public boolean shouldParse(Request request, Cluster.Node target) {
        if (target == self) {
            return true;
        }

        String param = request.getParameter("replicas=");
        int[] askFrom = parseAskFrom(param);

        return replicasManager.getAskReplicas(askFrom[1], target).contains(self);
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {

    }

    private int[] parseAskFrom(@Nullable String param) {
        int ask = -1, from = -1;

        if (param != null) {
            try {
                String[] args = param.split("/");
                ask = Integer.parseInt(args[0]);
                from = Integer.parseInt(args[1]);
            } catch (NumberFormatException ignored) {
                ask = -1;
            }
        }

        int maxReplicas = replicasManager.replicasCount;
        if (param == null || ask == -1 || from > maxReplicas || ask > from) {
            from = maxReplicas;
            ask = quorum(from);
        }
        return new int[]{ask, from};
    }

    private int quorum(int from) {
        return (int) Math.ceil(from * 0.5);
    }
}
