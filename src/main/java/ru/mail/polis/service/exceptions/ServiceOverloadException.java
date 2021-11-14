package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public final class ServiceOverloadException extends ServerRuntimeException {

    public static final ServiceOverloadException INSTANCE = new ServiceOverloadException();

    ServiceOverloadException() {
        super();
    }

    @Override
    public String description() {
        return "Service is overloaded";
    }

    @Override
    public String httpCode() {
        return Response.SERVICE_UNAVAILABLE;
    }
}
