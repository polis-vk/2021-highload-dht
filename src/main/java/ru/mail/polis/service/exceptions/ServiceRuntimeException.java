package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public class ServiceRuntimeException extends RuntimeException implements HttpException {

    public ServiceRuntimeException() {
        super();
    }

    public ServiceRuntimeException(Exception e) {
        super(e);
    }

    @Override
    public String description() {
        return "Internal server error";
    }

    @Override
    public String httpCode() {
        return Response.INTERNAL_ERROR;
    }
}
