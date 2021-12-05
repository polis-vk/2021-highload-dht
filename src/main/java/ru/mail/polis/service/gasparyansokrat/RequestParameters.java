package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;

import java.util.Arrays;
import java.util.Iterator;

public class RequestParameters {

    private final int httpMethod;
    private int ack;
    private int from;
    private final String startKey;
    private final String endKey;
    private byte[] bodyRequest;

    public RequestParameters(final Request request, final ClusterService clusterService) {
        this.httpMethod = request.getMethod();
        final String tmpKey = getParamRequest(request, "start");
        if (tmpKey.isEmpty()) {
            this.startKey = getParamRequest(request, "id");
        } else {
            this.startKey = tmpKey;
        }
        this.endKey = getParamRequest(request, "end");
        parseAckFrom(request, clusterService);
        this.bodyRequest = request.getBody();
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    public String getStartKey() {
        return startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    private String getParamRequest(final Request request, final String nameParam) {
        Iterator<String> params = request.getParameters(nameParam);
        return params.hasNext() ? params.next().substring(1) : "";
    }

    public int getHttpMethod() {
        return httpMethod;
    }

    public byte[] getBodyRequest() {
        return Arrays.copyOf(bodyRequest, bodyRequest.length);
    }

    public void setBodyRequest(byte[] bodyRequest) {
        this.bodyRequest = Arrays.copyOf(bodyRequest, bodyRequest.length);
    }

    private void parseAckFrom(final Request request, final ClusterService clusterService) {
        String ackFrom = getParamRequest(request, "replicas");
        if (ackFrom.isEmpty()) {
            this.ack = clusterService.getQuorumCluster();
            this.from = clusterService.getClusterSize();
        } else {
            String[] ackfrom = ackFrom.split("/");
            this.ack = Integer.parseInt(ackfrom[0]);
            this.from = Integer.parseInt(ackfrom[1]);
        }
    }
}
