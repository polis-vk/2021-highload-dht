package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Utils;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.sachuk.ilya.replication.Coordinator;
import ru.mail.polis.service.sachuk.ilya.replication.ReplicationInfo;
import ru.mail.polis.service.sachuk.ilya.sharding.Node;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeManager;
import ru.mail.polis.service.sachuk.ilya.sharding.NodeRouter;
import ru.mail.polis.service.sachuk.ilya.sharding.VNode;
import ru.mail.polis.service.sachuk.ilya.sharding.VNodeConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceImpl extends HttpServer implements Service {
    private final Logger logger = LoggerFactory.getLogger(ServiceImpl.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private final EntityRequestHandler entityRequestHandler;
    private final RequestPoolExecutor requestPoolExecutor = new RequestPoolExecutor(
            new ExecutorConfig(16, 1000)
    );

    private final ExecutorService coordinatorExecutor = Executors.newCachedThreadPool();
    private final NodeManager nodeManager;
    private final NodeRouter nodeRouter;
    private final Node node;
    private final Set<String> topology;
    private final Coordinator coordinator;

    public ServiceImpl(int port, DAO dao, Set<String> topology) throws IOException {
        super(configFrom(port));

        this.topology = topology;
        this.entityRequestHandler = new EntityRequestHandler(dao);
        this.node = new Node(port);
        this.nodeManager = new NodeManager(topology, new VNodeConfig(), node);
        this.nodeRouter = new NodeRouter(nodeManager);

        this.coordinator = new Coordinator(nodeManager, nodeRouter, entityRequestHandler);

        logger.info("Node with port " + port + " is started");
    }

    private static HttpServerConfig configFrom(int port) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();

        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};

        return httpServerConfig;
    }

    private Response entityRequest(Request request, String id, ReplicationInfo replicationInfo) {
        if (id == null || id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        ByteBuffer key = Utils.wrap(id);

        boolean isCoordinator = false;
        String timestamp = request.getHeader("Timestamp");
        if (timestamp == null) {
            isCoordinator = true;
            request.addHeader("Timestamp" + System.currentTimeMillis());
        } else {
            long secs = Long.parseLong(timestamp);
            logger.info(String.valueOf(new Timestamp(secs)));
        }

        String fromCoordinatorHeader = request.getHeader("FromCoordinator");
        boolean fromCoordinator = false;
        if (fromCoordinatorHeader == null) {
            request.addHeader("FromCoordinator" + "Yes");
        } else {
            fromCoordinator = true;
            logger.info(fromCoordinatorHeader);
        }

        //если запрос пришел от координатора, от возращаем ему респонс, чтобы он собрал всю инфу
        if (!isCoordinator) {
            logger.info("in block from coordinator");
            return entityRequestHandler.handle(request, id);
        } else {

            logger.info("in block IS coordinator");
            logger.info("COORDINATOR NODE IS: " + node.port);

//        if (isCoordinator) {
//            List<VNode> vnodeList = new ArrayList<>()
            SortedMap<Integer, VNode> vnodes = new TreeMap<>();
            List<Integer> currentPorts = new ArrayList<>();
            Integer hash = null;

            for (int i = 0; i < replicationInfo.from; i++) {
//                vnodeList.add(nodeManager.getNearVnodeNotInList(id, vnodeList));
                logger.info("In cycle i is : " + i);
                logger.info(String.valueOf(currentPorts));

                Pair<Integer, VNode> pair = nodeManager.getNearVNodeWithGreaterHash(id, hash, currentPorts);
                vnodes.put(pair.key, pair.value);
                hash = pair.key;
                currentPorts.add(pair.value.getPhysicalNode().port);
            }

            List<Response> responses = new ArrayList<>();
            List<Record> records = new ArrayList<>();

            logger.info("vnodeList size is: " + vnodes.size() + " and from is: " + replicationInfo.from);

            logger.info("nodes to handle: " + currentPorts);

            //берем респонсы
            for (VNode vNode : vnodes.values()) {
//                if (count == replicationInfo.ask) {
//                    coordinatorExecutor.execute(() -> {
//                        if (vNode.getPhysicalNode().port == node.port) {
//                            entityRequestHandler.handle(request, id);
//                        } else {
//                            nodeRouter.routeToNode(vNode, request);
//                        }
//                    });
//
//                    continue;
//                }

                Response response;
                if (vNode.getPhysicalNode().port == node.port) {
//                    logger.info("find current Node + " + vNode.getPhysicalNode().port);
                    logger.info("HANDLE BY CURRENT NODE: port :" + vNode.getPhysicalNode().port);

                    response = entityRequestHandler.handle(request, id);
                } else {
                    logger.info("HANDLE BY OTHER NODE: port :" + vNode.getPhysicalNode().port);

//                    logger.info("route to Node + " + vNode.getPhysicalNode().port);
                    response = nodeRouter.routeToNode(vNode, request);
                }

                if (response.getStatus() == 504 || response.getStatus() == 405) {
                    continue;
                }
                responses.add(response);
            }

            if (responses.size() < replicationInfo.ask) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }

            Response finalResponse = getFinalResponse(request, key, responses, records);

            logger.info("FINAL RESPONSE:" + finalResponse.getStatus());

            return finalResponse;
        }
    }

    private Response getFinalResponse(Request request, ByteBuffer key, List<Response> responses, List<Record> records) {
        Response finalResponse;
        if (request.getMethod() == Request.METHOD_GET) {
            for (Response response : responses) {
                int status = response.getStatus();

                if (status == 200 || status == 404) {

                    String timestampFromResponse = response.getHeader("Timestamp");
                    String tombstoneHeader = response.getHeader("Tombstone");

                    byte[] body = response.getBody();

                    logger.info("body size is: " + body.length);

                    ByteBuffer value = ByteBuffer.wrap(response.getBody());
                    if (timestampFromResponse == null) {
                        records.add(Record.of(key, value, Utils.timeStampToByteBuffer(0L)));
                    } else {
                        if (tombstoneHeader == null) {
                            logger.info("Tombstone header is null");
                            records.add(Record.of(key, value, Utils.timeStampToByteBuffer(Long.parseLong(timestampFromResponse))));
                        } else {
                            logger.info("Tombstone header is not null");
                            records.add(Record.tombstone(key, Utils.timeStampToByteBuffer(Long.parseLong(timestampFromResponse))));
                        }
                    }
                }
            }

            Record newestRecord = getNewestRecord(records);
            logger.info("is tombstone: " + newestRecord.isTombstone());
            finalResponse = getFinalResponseForGet(newestRecord);

        } else if (request.getMethod() == Request.METHOD_DELETE) {
            finalResponse = new Response(Response.ACCEPTED, Response.EMPTY);

        } else if (request.getMethod() == Request.METHOD_PUT) {
            finalResponse = new Response(Response.CREATED, Response.EMPTY);
        } else {
            finalResponse = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
        }
        return finalResponse;
    }

    private Response getFinalResponseForGet(Record newestRecord) {
        Response finalResponse;
        if (newestRecord.isTombstone()) {
            finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
        } else {
            if (Utils.byteBufferToTimestamp(newestRecord.getTimestamp()).getTime() == 0) {
                finalResponse = new Response(Response.NOT_FOUND, Response.EMPTY);
            } else {
                finalResponse = new Response(Response.OK, Utils.bytebufferToBytes(newestRecord.getValue()));
            }
        }
        return finalResponse;
    }

    private Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public void handleRequest(Request request, HttpSession session) {
        if (requestPoolExecutor.isQueueFull()) {
            try {
                session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        requestPoolExecutor.addTask(() -> {
            String path = request.getPath();
            Response response;
            switch (path) {
                case ENTITY_PATH:
                    String id = request.getParameter("id=");
                    String replicas = request.getParameter("replicas=");

                    ReplicationInfo replicationInfo = replicas == null
                            ? ReplicationInfo.from(topology.size())
                            : ReplicationInfo.from(replicas);

                    logger.info(replicas);
                    logger.info("ask=" + replicationInfo.ask + " and from=" + replicationInfo.from);

                    if (replicationInfo.ask > replicationInfo.from || replicationInfo.ask == 0) {
                        response = new Response(Response.BAD_REQUEST, Response.EMPTY);
                        break;
                    }

                    response = entityRequest(request, id, replicationInfo);
                    break;
                case STATUS_PATH:
                    response = status();
                    break;
                default:
                    try {
                        handleDefault(request, session);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return;
            }
            try {
                session.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public synchronized void stop() {
        super.stop();

        logger.info("Service with node:" + node.port + " is closed");

        nodeManager.close();
    }

    private Record getNewestRecord(List<Record> records) {
        logger.info("records size in getNewestValue:" + records.size());
        records.sort((o1, o2) -> {
            Timestamp timestamp1 = Utils.byteBufferToTimestamp(o1.getTimestamp());
            Timestamp timestamp2 = Utils.byteBufferToTimestamp(o2.getTimestamp());

            int compare = timestamp2.compareTo(timestamp1);

            if (compare != 0) {
                return compare;
            }

            if (o1.getValue() == null && o2.getValue() == null) {
                return 1;
            }

            if (o1.getValue() == null) {
                return -1;
            }
            if (o2.getValue() == null) {
                return 1;
            }

            if (o1.getValue().remaining() == 0) {
                return -1;
            }

            if (o2.getValue().remaining() == 0) {
                return 1;
            }

//            String value1 = Utils.toString(o1.getValue().position(0).duplicate());
//            String value2 = Utils.toString(o2.getValue().position(0).duplicate());


            return o2.getValue().compareTo(o1.getValue());
        });

        return records.get(0);
    }
}
