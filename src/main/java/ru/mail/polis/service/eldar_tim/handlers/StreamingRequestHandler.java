package ru.mail.polis.service.eldar_tim.handlers;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.service.eldar_tim.ServiceResponse;
import ru.mail.polis.service.eldar_tim.StreamingHttpSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Supplier;

public abstract class StreamingRequestHandler implements BaseRequestHandler {

    @Nonnull
    public abstract StreamingServiceResponse handleRequest(Request request);

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        StreamingServiceResponse response = handleRequest(request);
        if (response.supplier != null) {
            var streamingSession = (StreamingHttpSession) session;
            streamingSession.sendStreamingResponse(response.raw(), response.supplier);
        } else {
            session.sendResponse(response.raw());
        }
    }

    public static class StreamingServiceResponse extends ServiceResponse {
        private final Supplier<byte[]> supplier;

        private StreamingServiceResponse(
                @Nonnull Response response, long timestamp, @Nullable Supplier<byte[]> supplier
        ) {
            super(response, timestamp);
            this.supplier = supplier;
        }

        public static StreamingServiceResponse of(@Nonnull Response response, Supplier<byte[]> supplier) {
            return new StreamingServiceResponse(response, -1, supplier);
        }

        public static StreamingServiceResponse of(@Nonnull Response response) {
            return new StreamingServiceResponse(response, -1, null);
        }
    }
}
