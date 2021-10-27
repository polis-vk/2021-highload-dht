package ru.mail.polis.service.avightclav;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class RemoteService extends HttpClient {
    public String me;
    public String remote;
    public RemoteService(String conn, String me, String remote) {
        super(new ConnectionString(conn));
        this.me = me;
        this.remote = remote;
    }

    public Response getEntity(final Request request) {
        try {
            return this.get(this.remote, Arrays.stream(request.getHeaders()).filter(Objects::nonNull).toArray(String[]::new));
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(Response.INTERNAL_ERROR, "Can't connect to remote".getBytes());
        }
    }

    public Response put(final Request request) {
        try {
            return this.put(this.remote, request.getBody(), Arrays.stream(request.getHeaders()).filter(Objects::nonNull).toArray(String[]::new));
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(Response.INTERNAL_ERROR, "Can't connect to remote".getBytes());
        }
    }

    public Response delete(final Request request) {
        try {
            return this.delete(this.remote, Arrays.stream(request.getHeaders()).filter(Objects::nonNull).toArray(String[]::new));
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(Response.INTERNAL_ERROR, "Can't connect to remote".getBytes());
        }
    }
}
