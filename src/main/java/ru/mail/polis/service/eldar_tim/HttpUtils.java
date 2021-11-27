package ru.mail.polis.service.eldar_tim;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import ru.mail.polis.Cluster;
import ru.mail.polis.service.eldar_tim.handlers.ReplicableRequestHandler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executor;

public final class HttpUtils {

    private static final String[] LOCAL_HEADERS = {
            ServiceResponse.HEADER_TIMESTAMP,
            ReplicableRequestHandler.HEADER_HANDLE_LOCALLY
    };

    private static final HttpRequest.BodyPublisher NO_BODY = HttpRequest.BodyPublishers.noBody();

    private HttpUtils() {
        // No need.
    }

    public static HttpClient createClient(Executor executor) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(3))
                .proxy(HttpClient.Builder.NO_PROXY)
                .executor(executor)
                .build();
    }

    public static HttpRequest mapRequest(Request request, Cluster.Node target) {
        HttpRequest.BodyPublisher bodyPublisher = NO_BODY;
        byte[] body = request.getBody();
        if (body != null && body.length > 0) {
            bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(body);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(target.uri + request.getURI()))
                .method(request.getMethodName(), bodyPublisher)
                .timeout(Duration.ofSeconds(3));

        for (var header : LOCAL_HEADERS) {
            String headerValue = request.getHeader(header);
            if (headerValue != null) {
                builder.setHeader(header, headerValue.substring(2));
            }
        }

        return builder.build();
    }

    public static Response mapResponse(HttpResponse.ResponseInfo responseInfo, byte[] body) {
        return new Response(String.valueOf(responseInfo.statusCode()), body);
    }

    public static void sendResponse(Logger log, HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            log.debug("Unable to send response", e);
        }
    }

    public static void sendError(Logger log, HttpSession session, String code, String message) {
        try {
            session.sendError(code, message);
        } catch (IOException e) {
            log.debug("Unable to send error: {}", message, e);
        }
    }
}
