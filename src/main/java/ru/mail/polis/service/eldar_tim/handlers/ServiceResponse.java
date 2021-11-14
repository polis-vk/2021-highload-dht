package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.Response;

import javax.annotation.Nonnull;

public final class ServiceResponse {

    public static final String HEADER_TIMESTAMP = "Service-Data-Timestamp";

    private final Response response;
    private final long timestamp;

    private ServiceResponse(@Nonnull Response response, long timestamp) {
        this.response = response;
        this.timestamp = timestamp;
    }

    public Response transform() {
        if (timestamp > 0) {
            response.addHeader(HEADER_TIMESTAMP + ": " + timestamp);
        }
        return response;
    }

    public static ServiceResponse of(@Nonnull Response response, long timestamp) {
        return new ServiceResponse(response, timestamp);
    }

    public static ServiceResponse of(@Nonnull Response response) {
        return of(response, -1);
    }
}
