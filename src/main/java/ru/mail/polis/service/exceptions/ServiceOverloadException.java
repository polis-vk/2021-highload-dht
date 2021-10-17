package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public class ServiceOverloadException extends ServiceRuntimeException {

    public ServiceOverloadException() {
        super();
    }

    public ServiceOverloadException(Exception e) {
        super(e);
    }

    @Override
    public String description() {
        return "Server overloaded";
    }

    @Override
    public String httpCode() {
        return Response.SERVICE_UNAVAILABLE;
    }
}
