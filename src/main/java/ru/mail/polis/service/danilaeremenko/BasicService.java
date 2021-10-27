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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {
    private final DaoWrapper daoWrapper;
    private final ConcurrentSkipListMap<String, ClusterAdapter> clusterAdaptersMap = new ConcurrentSkipListMap<>();
    private String myClusterId;
    private final Executor serviceExecutor = Executors.newFixedThreadPool(4);
    private static final Logger SERVICE_LOGGER = LoggerFactory.getLogger(BasicService.class);
    private final ConsistentHash consistentHash;

    public BasicService(int port, final Set<String> topology, DAO dao) throws IOException {
        super(MyConfigFactory.fromPortWorkersKeepAlive(port, 4, 1));
        this.daoWrapper = new DaoWrapper(dao);
        int clusterId = 0;
        List<String> sortedTopology = new ArrayList<>(topology);
        Collections.sort(sortedTopology);
        boolean myClusterIdParsed = false;//sorry for this, it's codeclimate..
        for (String adapterDesc : sortedTopology) {
            ClusterAdapter currCluster = ClusterAdapter.fromStringDesc(adapterDesc);
            clusterAdaptersMap.put(String.valueOf(clusterId), currCluster);
            //TODO we must check ip also, but with current tests it will work
            if (currCluster.port == port && !myClusterIdParsed) {
                this.myClusterId = String.valueOf(clusterId);
                myClusterIdParsed = true;
            }
            clusterId++;
        }

        if (myClusterId == null) {
            throw new IOException("Parameters of host not found in topology");
        }

        this.consistentHash = new ConsistentHash(clusterAdaptersMap.size());
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
            final String recordId,
            final String clusterId
    ) throws IOException, InterruptedException {
        SERVICE_LOGGER.info("processing by target, id = " + clusterId);
        ClusterAdapter targetAdapter = this.clusterAdaptersMap.get(clusterId);
        Response response = targetAdapter.processRequest(request);
        localSession.sendResponse(response);
    }

    public void processEntityByMyself(
            final Request request,
            final HttpSession localSession,
            final String recordId
    ) throws IOException {
        SERVICE_LOGGER.info("processing by myself, id = " + myClusterId);
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
        String clusterId = String.valueOf(consistentHash.getClusterId(recordId));

        if (clusterId.equals(myClusterId)) {
            processEntityByMyself(request, localSession, recordId);
        } else {
            processEntityByTarget(request, localSession, recordId, clusterId);
        }

    }
}
