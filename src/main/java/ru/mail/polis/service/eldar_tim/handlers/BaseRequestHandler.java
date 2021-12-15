package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.RequestHandler;
import one.nio.http.Response;
import ru.mail.polis.service.eldar_tim.ServiceResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

public interface BaseRequestHandler extends RequestHandler {

    ServiceResponse METHOD_NOT_ALLOWED = ServiceResponse.of(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));

    @Nonnull
    default ServiceResponse handleRequest(Request request) {
        return METHOD_NOT_ALLOWED;
    }

    @Override
    default void handleRequest(Request request, HttpSession session) throws IOException {
        ServiceResponse response = handleRequest(request);
        session.sendResponse(response.raw());
    }
}
