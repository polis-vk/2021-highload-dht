package ru.mail.polis.service.igorsamokhin;

import one.nio.http.Response;
import one.nio.util.Utf8;

public final class UtilResponses {
    private UtilResponses() {
    }

    private static Response emptyResponse(String resultCode) {
        return new Response(resultCode, Response.EMPTY);
    }

    private static Response responseWithMessage(String resultCode, String message) {
        return new Response(resultCode, Utf8.toBytes(message));
    }

    public static Response badRequest(String message) {
        return responseWithMessage(Response.BAD_REQUEST, message);
    }

    public static Response badRequest() {
        return emptyResponse(Response.BAD_REQUEST);
    }

    public static Response serviceUnavailableRequest() {
        return emptyResponse(Response.SERVICE_UNAVAILABLE);
    }

    public static Response notFoundResponse() {
        return emptyResponse(Response.NOT_FOUND);
    }

    public static Response acceptedResponse() {
        return emptyResponse(Response.ACCEPTED);
    }

    public static Response createdResponse() {
        return emptyResponse(Response.CREATED);
    }
}


