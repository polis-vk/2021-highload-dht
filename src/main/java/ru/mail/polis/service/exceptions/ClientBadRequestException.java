package ru.mail.polis.service.exceptions;

import one.nio.http.Response;

public class ClientBadRequestException extends ServerRuntimeException implements HttpException {

    public ClientBadRequestException(Exception e) {
        super(e);
    }

    @Override
    public String description() {
        return "Bad request: " + getMessage();
    }

    @Override
    public String httpCode() {
        return Response.BAD_REQUEST;
    }
}
