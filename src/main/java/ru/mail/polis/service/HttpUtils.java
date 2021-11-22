package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.Cluster;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executor;

public class HttpUtils {

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
        HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.noBody();
        byte[] body = request.getBody();
        if (body != null && body.length != 0) {
            bodyPublisher = HttpRequest.BodyPublishers.ofByteArray(request.getBody());
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(target.uri + request.getURI()))
                .method(request.getMethodName(), bodyPublisher);

        for (var header : request.getHeaders()) {
            if (header == null) {
                continue;
            }
            try {
                int i = header.indexOf(':');
                builder.header(header.substring(0, i), header.substring(i + 1).trim());
            } catch (IllegalArgumentException | IndexOutOfBoundsException ignored) {
                // Ignore Java restricted headers.
            }
        }
        return builder.build();
    }

    public static Response mapResponse(HttpResponse<byte[]> response) {
        return new Response(String.valueOf(response.statusCode()), response.body());
    }

    public static Response mapResponseInfo(HttpResponse.ResponseInfo responseInfo, byte[] body) {
        return new Response(String.valueOf(responseInfo.statusCode()), body);
    }
}
