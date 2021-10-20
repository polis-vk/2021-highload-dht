package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public class ServerRuntimeException extends RuntimeException implements HttpException {

    public ServerRuntimeException() {
        super();
    }

    public ServerRuntimeException(Throwable cause) {
        super(cause);
    }

    @Override
    public String description() {
        return "Internal server error: " + getMessage();
    }

    @Override
    public String httpCode() {
        return Response.INTERNAL_ERROR;
    }
}
