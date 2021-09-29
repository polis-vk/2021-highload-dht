package ru.mail.polis.service.lucas_mbele;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class HttpRestService extends HttpServer implements Service {
    private final DAO dao; // Our Data Object Access
    public HttpRestService (final int port, final DAO dao) throws IOException {
        super(serviceConfig(port));
        this.dao = dao;
    }

    public static HttpServerConfig serviceConfig(int port)
    {
        // Minimal configuration of our service
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ServiceUtils.acceptors(port)};
        return config;
    }
     // We check if our service works
     @Path("/v0/status")
     public Response status(Request request)
     {
         //Obviously we know that this request implies a GET Method, but for the purpose we set it
         if (request.getMethod() == Request.METHOD_GET) // TRUE
         {
             return Response.ok(Response.OK); //Code 202
         }
         else
         {
             return new Response(Response.SERVICE_UNAVAILABLE,Response.EMPTY);
         }

     }
    //METHODS - GET/PUT/DELETE
    @Path("/v0/entity")
    public Response entity(Request request, @Param(value = "id",required = true) String id)
    {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        // We handle all 3 methods
        switch (request.getMethod())
        {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_PUT:
                return put(id,request.getBody());
            case Request.METHOD_DELETE:
                return delete(id);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED,"Method not allowed".getBytes(StandardCharsets.UTF_8));
        }
    }
    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> keyIterator = dao.range(key,DAO.nextKey(key)); // A key range containing ids whose start from the current id
        if (keyIterator.hasNext())
         {
            Record record = keyIterator.next();
            return new Response(Response.OK , ServiceUtils.extractBytesBuffer(record.getValue()));
         }
        else 
         {
            return new Response(Response.NOT_FOUND,Response.EMPTY);
         }
    }
    private Response put(String id, byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key,value));
        return new Response(Response.CREATED,Response.EMPTY);
    }

    private Response delete(String id)
    {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST,Response.EMPTY));
    }
}
