package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;
    private final Executor longTasksService = Executors.newFixedThreadPool(4);//TODO so, we need to think about order..

    public BasicService(int port, DAO dao) throws IOException {
        super(MyConfigFactory.fromPortWorkers(port, 4));
        this.daoWrapper = new DaoWrapper(dao);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, "Not found".getBytes(StandardCharsets.UTF_8)));
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("Okay bro");
    }

    @Path("/v0/entity")
    public void entity(
            final Request request,
            @Param(value = "id", required = true) final String id,
            HttpSession localSession
    ) throws IOException {
        if (id.isBlank()) {
            Response response = new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
            localSession.sendResponse(response);
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                this.longTasksService.execute(() -> {
                            Response response = this.daoWrapper.getEntity(id);
                            try {
                                localSession.sendResponse(response);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
                break;
            case Request.METHOD_PUT:
                this.longTasksService.execute(() -> {
                            Response response = this.daoWrapper.putEntity(id, request.getBody());
                            try {
                                localSession.sendResponse(response);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
                break;
            case Request.METHOD_DELETE:
                this.longTasksService.execute(() -> {
                            Response response = this.daoWrapper.deleteEntity(id);
                            try {
                                localSession.sendResponse(response);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
                break;
            default:
                Response response = new Response(Response.BAD_REQUEST, "Undefined method".getBytes(StandardCharsets.UTF_8));
                localSession.sendResponse(response);
        }

    }
}
