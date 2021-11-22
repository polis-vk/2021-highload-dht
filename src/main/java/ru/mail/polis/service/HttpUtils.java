package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;

import java.net.Authenticator;
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
                .authenticator(Authenticator.getDefault())
                .executor(executor)
                .build();
    }

    public static HttpRequest mapRequest(Request request) {
        return HttpRequest.newBuilder()
                .uri(URI.create(request.getURI()))
                .headers(request.getHeaders())
                .method(request.getMethodName(), HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .build();
    }

    public static Response mapResponse(HttpResponse<byte[]> response) {
        return new Response(response.statusCode() + "", response.body()); // FIXME: status
    }

    public static Response mapResponseInfo(HttpResponse.ResponseInfo responseInfo, byte[] body) {
        return new Response(responseInfo.statusCode() + "", body); // FIXME: status
    }
}
