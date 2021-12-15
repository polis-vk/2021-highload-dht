package ru.mail.polis.service.eldar_tim;

import one.nio.http.Response;

import javax.annotation.Nonnull;

public class ServiceResponse {

    public static final String HEADER_TIMESTAMP = "Service-Data-Timestamp";

    protected final Response response;
    public final long timestamp;

    protected ServiceResponse(@Nonnull Response response, long timestamp) {
        this.response = response;
        this.timestamp = timestamp;
    }

    public Response timestamped() {
        if (timestamp > 0) {
            response.addHeader(HEADER_TIMESTAMP + ": " + timestamp);
        }
        return response;
    }

    public Response raw() {
        return response;
    }

    public static ServiceResponse of(@Nonnull Response response, long timestamp) {
        return new ServiceResponse(response, timestamp);
    }

    public static ServiceResponse of(@Nonnull Response response) {
        return of(response, -1);
    }
}
