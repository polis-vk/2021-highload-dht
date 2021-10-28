package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;
    private final Executor serviceExecutor = Executors.newFixedThreadPool(4);
    private static final Logger SERVICE_LOGGER = LoggerFactory.getLogger(BasicService.class);
    private final ConsistentHash consistentHash;

    public BasicService(int port, final Set<String> topology, DAO dao) throws IOException {
        super(MyConfigFactory.fromPortWorkersKeepAlive(port, 4, 1));
        this.daoWrapper = new DaoWrapper(dao);
        this.consistentHash = new ConsistentHash(topology);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("Okay bro");
    }

    @Override
    public void handleDefault(Request request, HttpSession localSession) {
        serviceExecutor.execute(() -> {
            try {
                String path = request.getPath();
                if ("/v0/entity".equals(path)) {
                    processEntity(request, localSession);
                } else {
                    localSession.sendResponse(
                            new Response(
                                    Response.BAD_REQUEST,
                                    "Not found".getBytes(StandardCharsets.UTF_8))
                    );
                }
            } catch (IOException | InterruptedException e) {
                SERVICE_LOGGER.error("IOException caught handleDefault", e);
            }
        });
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*----------------------------------------------- PROCESS ENTITY -------------------------------------------------*/
    /*----------------------------------------------------------------------------------------------------------------*/
    public void processEntityByTarget(
            final Request request,
            final HttpSession localSession,
            final ClusterAdapter targetAdapter
    ) throws IOException, InterruptedException {
        SERVICE_LOGGER.debug("processing by target host {}", targetAdapter);
        Response response = targetAdapter.processRequest(request);
        localSession.sendResponse(response);
    }

    public void processEntityByMyself(
            final Request request,
            final HttpSession localSession,
            final String recordId,
            final ClusterAdapter targetAdapter
    ) throws IOException {
        SERVICE_LOGGER.debug("processing by myself {}", targetAdapter);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                localSession.sendResponse(this.daoWrapper.getEntity(recordId));
                return;
            case Request.METHOD_PUT:
                localSession.sendResponse(this.daoWrapper.putEntity(recordId, request.getBody()));
                return;
            case Request.METHOD_DELETE:
                localSession.sendResponse(this.daoWrapper.deleteEntity(recordId));
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

    public void processEntity(
            final Request request,
            final HttpSession localSession
    ) throws IOException, InterruptedException {
        final String recordId = request.getParameter("id");
        if (recordId == null || recordId.equals("=")) {
            localSession.sendResponse(
                    new Response(
                            Response.BAD_REQUEST,
                            "Undefined method".getBytes(StandardCharsets.UTF_8)
                    )
            );
            return;
        }

        ClusterAdapter targetAdapter = consistentHash.getClusterAdapter(recordId);
        //TODO we need to know and compare our ip also
        if (targetAdapter.getPort() == this.port) {
            processEntityByMyself(request, localSession, recordId, targetAdapter);
        } else {
            processEntityByTarget(request, localSession, targetAdapter);
        }

    }
}
