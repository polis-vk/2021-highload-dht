package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.Response;

public final class ResponseUtils {
    public static final String TIMESTAMP_HEADER = "Timestamp: ";
    public static final String TOMBSTONE_HEADER = "Tombstone: ";

    private ResponseUtils() {

    }

    public static Response addTimeStampHeader(Response response, long secs) {
        response.addHeader(ResponseUtils.TIMESTAMP_HEADER + secs);

        return response;
    }

    public static Response addTimeStampHeaderAndTombstone(Response response, long secs) {
        response.addHeader(ResponseUtils.TIMESTAMP_HEADER + secs);
        response.addHeader(ResponseUtils.TOMBSTONE_HEADER);

        return response;
    }
}
