package ru.mail.polis;

import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;

import java.io.IOException;

public interface ClusterProxySystem {

    Response invokeEntityRequest(String entityId, Request request)
            throws HttpException, IOException, PoolException, InterruptedException;

}
