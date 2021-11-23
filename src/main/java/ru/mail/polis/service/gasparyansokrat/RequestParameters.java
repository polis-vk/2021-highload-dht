package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.Request;

import java.util.Iterator;

public class RequestParameters {

    private int httpMethod;
    private int ack;
    private int from;
    private String id;
    private byte[] bodyRequest;

    public RequestParameters(final Request request, final ClusterService clusterService) {
        this.httpMethod = request.getMethod();
        this.id = getParamRequest(request, "id");
        String ackFrom = getParamRequest(request, "replicas");
        if (ackFrom.isEmpty()) {
            this.ack = clusterService.getQuorumCluster();
            this.from = clusterService.getClusterSize();
        } else {
            String[] ackfrom = ackFrom.split("/");
            this.ack = Integer.parseInt(ackfrom[0]);
            this.from = Integer.parseInt(ackfrom[1]);
        }
        this.bodyRequest = request.getBody();
    }

    public RequestParameters(final int ack, final int from,
                             final String id, final int httpMethod) {
        this.httpMethod = httpMethod;
        this.ack = ack;
        this.from = from;
        this.id = id;
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    public String getId() {
        return id;
    }

    public void setAck(final int ack) {
        this.ack = ack;
    }

    public void setFrom(final int from) {
        this.from = from;
    }

    public void setId(final String id) {
        this.id = id;
    }

    private String getParamRequest(final Request request, final String nameParam) {
        Iterator<String> params = request.getParameters(nameParam);
        return params.hasNext() ? params.next().substring(1) : "";
    }

    public int getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(int method) {
        this.httpMethod = method;
    }

    public byte[] getBodyRequest() {
        return bodyRequest;
    }

    public void setBodyRequest(byte[] bodyRequest) {
        this.bodyRequest = bodyRequest;
    }
}
