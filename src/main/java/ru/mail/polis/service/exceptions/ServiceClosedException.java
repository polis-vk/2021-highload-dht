package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

@SuppressWarnings("PMD.AtLeastOneConstructor")
public class ServiceClosedException extends ServiceRuntimeException {

    @Override
    public String description() {
        return "Service is closed";
    }

    @Override
    public String httpCode() {
        return Response.SERVICE_UNAVAILABLE;
    }
}
