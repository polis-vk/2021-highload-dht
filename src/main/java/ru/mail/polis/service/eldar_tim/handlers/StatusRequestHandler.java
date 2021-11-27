package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.eldar_tim.ServiceResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StatusRequestHandler extends RequestHandler {

    public StatusRequestHandler(HandlerContext context) {
        super(context);
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
