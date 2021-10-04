package ru.mail.polis.service.lucas_mbele;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class HttpRestService extends HttpServer implements Service {
    private final DAO dao; // Our Data Object Access
    private final ThreadPoolExecutor executor;

    public HttpRestService(final int port, final DAO dao,final int poolSize) throws IOException {
        super(serviceConfig(port));
        this.dao = dao;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);
    }

    public static HttpServerConfig serviceConfig(int port) {
        // Minimal configuration of our service
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ServiceUtils.acceptors(port)};
        return config;
    }

    // We check if our service works
    @Path("/v0/status")
    public Response status(Request request) {
        //Obviously we know that this request implies a GET Method, but for the purpose we set it
        if (request.getMethod() == Request.METHOD_GET) {
            return Response.ok(Response.OK); //Code 202
        } else {
            return new Response(Response.SERVICE_UNAVAILABLE,Response.EMPTY);
        }

    }

    //METHODS - GET/PUT/DELETE
    @Path("/v0/entity")
    public void entity(Request request,HttpSession session,@Param(value = "id",required = true) String id) {
        executor.execute(() -> {
            Response response;
            if (id.isBlank()) {
                response = new Response(Response.BAD_REQUEST, Response.EMPTY);
            } else {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        response = get(id);
                    case Request.METHOD_PUT:
                        response = put(id, request.getBody());
                    case Request.METHOD_DELETE:
                        response = delete(id)
                    default:
                        response = new Response(Response.METHOD_NOT_ALLOWED,
                                "Method not allowed".getBytes(StandardCharsets.UTF_8));
                }
            }
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
          });
    }

    private Response get(String id) {
        Response response;
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> keyIterator = dao.range(key,DAO.nextKey(key)); //A key range ids started from current id
        if (keyIterator.hasNext()) {
            Record record = keyIterator.next();
            response = new Response(Response.OK,ServiceUtils.extractBytesBuffer(record.getValue()));
        } else {
            response = new Response(Response.NOT_FOUND,Response.EMPTY);
        }
            return response;
    }

    private Response put(String id, byte[] body) {
            ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
            ByteBuffer value = ByteBuffer.wrap(body);
            dao.upsert(Record.of(key, value));
            return new Response(Response.CREATED, Response.EMPTY);
        }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED,Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) {
        this.executor.execute(() -> {
            try {
                session.sendResponse(new Response(Response.BAD_REQUEST,Response.EMPTY));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
