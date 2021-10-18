package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public class ServiceClosedException extends ServerRuntimeException {

    @Override
    public String description() {
        return "Service is closed";
    }

    @Override
    public String httpCode() {
        return Response.SERVICE_UNAVAILABLE;
    }
}
