package ru.mail.polis.service.avightclav;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import ru.mail.polis.service.Service;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RemoteService implements Service {
    private final HttpClient client;
    private static final String REMOTE_CONNECTION_ERROR_MESSAGE = "Can't connect to remote";

    public RemoteService(String conn) {
        this.client = new HttpClient(new ConnectionString(conn));
    }

    @Override
    public Response get(@Nonnull String id) {
        try {
            return client.get(ServiceUrls.ENTITY_URI_PREFIX + "?id" + id);
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(
                    Response.INTERNAL_ERROR,
                    REMOTE_CONNECTION_ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    @Override
    public Response put(@Nonnull String id, @Nonnull byte[] body) {
        try {
            return client.put(ServiceUrls.ENTITY_URI_PREFIX + "?id" + id, body);
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(
                    Response.INTERNAL_ERROR, REMOTE_CONNECTION_ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    @Override
    public Response delete(@Nonnull String id) {
        try {
            return client.delete(ServiceUrls.ENTITY_URI_PREFIX + "?id" + id);
        } catch (InterruptedException | HttpException | IOException | PoolException e) {
            return new Response(
                    Response.INTERNAL_ERROR,
                    REMOTE_CONNECTION_ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    @Override
    public Response status() {
        try {
            return client.get(ServiceUrls.STATUS_URI_PREFIX);
        } catch (InterruptedException | PoolException | IOException | HttpException e) {
            return new Response(
                    Response.INTERNAL_ERROR,
                    REMOTE_CONNECTION_ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
