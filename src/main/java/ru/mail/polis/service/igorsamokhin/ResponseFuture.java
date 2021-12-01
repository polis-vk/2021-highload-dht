package ru.mail.polis.service.igorsamokhin;

import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ResponseFuture {
    private final Logger logger = LoggerFactory.getLogger(ResponseFuture.class);
    private Response response;
    private CompletableFuture<HttpResponse<byte[]>> future;
    private CompletableFuture<Void> startLever;
    private final ExecutorService executor;

    public ResponseFuture(CompletableFuture<HttpResponse<byte[]>> future,
                          CompletableFuture<Void> startLever,
                          ExecutorService executor) {
        this.future = future;
        this.startLever = startLever;
        this.executor = executor;
    }

    public ResponseFuture(Response response, ExecutorService executor) {
        this.response = response;
        this.executor = executor;
    }

    public void thenCompose(ResponseTask task) {
        if (response != null) {
            task.run(response);
            return;
        }

        this.future = this.future.whenComplete((r, ex) -> {
            if (ex == null) {
                Response tmp = response;
                task.run(tmp == null ? responseFrom(r) : tmp);
                return;
            }

            logger.error("Something went wrong on handling task", ex);
            response = Utils.serviceUnavailableResponse();
            task.run(response);
        });

        startLever.completeAsync(() -> null, executor);
    }

    private Response responseFrom(HttpResponse<byte[]> r) {
        Response tmp = new Response(Integer.toString(r.statusCode()), r.body());
        List<String> headers = r.headers().allValues(Utils.TOMBSTONE_HEADER);
        if (!headers.isEmpty()) {
            tmp.addHeader(Utils.TOMBSTONE_HEADER_ONE_NIO);
        }
        return tmp;
    }

    public Response getResponse() {
        if (response != null) {
            return response;
        }

        Response currentResponse;
        try {
            HttpResponse<byte[]> futureResponse = this.future.get();
            currentResponse = responseFrom(futureResponse);
        } catch (Exception e) {
            logger.info("Something wrong in getResponse", e);
            currentResponse = Utils.serviceUnavailableResponse();
        }
        return currentResponse;
    }

    @FunctionalInterface
    interface ResponseTask {
        void run(Response response);
    }
}
