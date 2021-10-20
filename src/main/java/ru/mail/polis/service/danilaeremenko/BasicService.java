package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;
    private final Executor serviceExecutor = Executors.newFixedThreadPool(4);
    private static final Logger SERVICE_LOGGER = LoggerFactory.getLogger(BasicService.class);

    public BasicService(int port, DAO dao) throws IOException {
        super(MyConfigFactory.fromPortWorkersKeepAlive(port, 4, 1));
        this.daoWrapper = new DaoWrapper(dao);
    }

    @Override
    public void handleDefault(Request request, HttpSession localSession) {
        serviceExecutor.execute(() -> {
            try {
                String path = request.getPath();
                switch (path) {
                    case "/v0/status":
                        processStatus(localSession);
                        return;
                    case "/v0/entity":
                        processEntity(request, localSession);
                        return;
                    default:
                        localSession.sendResponse(
                                new Response(
                                        Response.BAD_REQUEST,
                                        "Not found".getBytes(StandardCharsets.UTF_8))
                        );
                        break;
                }
            } catch (IOException e) {
                SERVICE_LOGGER.error("IOException caught handleDefault", e);
            }
        });
    }

    public void processStatus(HttpSession localSession) throws IOException {
        localSession.sendResponse(Response.ok("Okay bro"));
    }

    public void processEntity(
            final Request request,
            HttpSession localSession
    ) throws IOException {
        final String id = request.getParameter("id");
        if (id == null || id.equals("=")) {
            localSession.sendResponse(
                    new Response(
                            Response.BAD_REQUEST,
                            "Undefined method".getBytes(StandardCharsets.UTF_8)
                    )
            );
            return;
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                localSession.sendResponse(this.daoWrapper.getEntity(id));
                return;
            case Request.METHOD_PUT:
                localSession.sendResponse(this.daoWrapper.putEntity(id, request.getBody()));
                return;
            case Request.METHOD_DELETE:
                localSession.sendResponse(this.daoWrapper.deleteEntity(id));
                return;
            default:
                localSession.sendResponse(
                        new Response(
                                Response.BAD_REQUEST,
                                "Undefined method".getBytes(StandardCharsets.UTF_8)
                        )
                );
                break;
        }

    }
}
