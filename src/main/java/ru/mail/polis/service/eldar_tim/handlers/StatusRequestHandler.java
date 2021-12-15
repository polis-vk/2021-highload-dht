package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.eldar_tim.ServiceResponse;

import javax.annotation.Nonnull;

public class StatusRequestHandler implements BaseRequestHandler {

    public StatusRequestHandler() {
        // No need.
    }

    @Nonnull
    @Override
    public ServiceResponse handleRequest(Request request) {
        return ServiceResponse.of(Response.ok(Response.OK));
    }
}
