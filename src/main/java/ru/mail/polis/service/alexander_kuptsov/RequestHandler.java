package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.service.alexander_kuptsov.sharding.ClusterNodeHandler;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.*;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.Fnv1Algorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final ClusterNodeHandler clusterNodeHandler;
    private final InternalDaoService internalDaoService;

    public RequestHandler(Set<String> topology, final int selfPort, InternalDaoService internalDaoService) {
        IHashAlgorithm hashAlgorithm = new Fnv1Algorithm();
        IDistributionAlgorithm maglevAlg = new MaglevAlgorithm(hashAlgorithm);
        this.clusterNodeHandler = new ClusterNodeHandler(topology, selfPort, maglevAlg);
        this.internalDaoService = internalDaoService;
    }

    public Response entity(String id, int requestMethod, Request request) {
        if (!isSelfRequest(id)) {
            return handleNotSelfRequest(id, request);
        }

        switch (requestMethod) {
            case Request.METHOD_GET:
                return internalDaoService.get(id);
            case Request.METHOD_PUT:
                return internalDaoService.put(id, request.getBody());
            case Request.METHOD_DELETE:
                return internalDaoService.delete(id);
            default:
                return new Response(
                        Response.METHOD_NOT_ALLOWED,
                        "Wrong method. Try GET/PUT/DELETE".getBytes(StandardCharsets.UTF_8)
                );
        }
    }

    private Response handleNotSelfRequest(String id, Request request) {
        try {
            return clusterNodeHandler.getServer(id).invoke(request);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            logger.error("Can't redirect request", e);
            return new Response(Response.BAD_GATEWAY, Response.EMPTY);
        }
    }

    private boolean isSelfRequest(String id) {
        return clusterNodeHandler.isSelfNode(id);
    }
}
