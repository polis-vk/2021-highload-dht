package ru.mail.polis.service.igorsamokhin;

import one.nio.http.Response;

public final class Utils {
    public static final String TOMBSTONE_HEADER = "Tombstone";
    public static final String PROXY_HEADER = "Proxy";
    public static final String TOMBSTONE_HEADER_ONE_NIO = TOMBSTONE_HEADER + ": ";
    public static final String PROXY_HEADER_ONE_NIO = PROXY_HEADER + ": ";

    private Utils() {
    }

    public static Response emptyResponse(String resultCode) {
        return new Response(resultCode, Response.EMPTY);
    }

    public static Response responseWithMessage(String resultCode, String message) {
        return new Response(resultCode + " " + message, Response.EMPTY);
    }

    public static Response badRequest(String message) {
        return responseWithMessage(Response.BAD_REQUEST, message);
    }

    public static Response badRequest() {
        return emptyResponse(Response.BAD_REQUEST);
    }

    public static Response serviceUnavailableResponse() {
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


