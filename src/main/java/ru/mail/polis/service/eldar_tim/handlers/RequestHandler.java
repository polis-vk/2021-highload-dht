package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.eldar_tim.ServiceResponse;

import javax.annotation.Nonnull;

public abstract class RequestHandler extends ReplicableRequestHandler {

    private static final ServiceResponse METHOD_NOT_ALLOWED =
            ServiceResponse.of(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));

    public RequestHandler(HandlerContext context) {
        super(context);
    }

    @Nonnull
    @Override
    protected ServiceResponse handleRequest(Request request) {
        return METHOD_NOT_ALLOWED;
    }
}
