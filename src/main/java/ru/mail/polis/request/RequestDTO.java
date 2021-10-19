package ru.mail.polis.request;

import one.nio.http.HttpSession;
import one.nio.http.Request;

public class RequestDTO {

    private final Request request;
    private final HttpSession session;

    public RequestDTO(Request request, HttpSession session) {
        this.request = request;
        this.session = session;
    }

    public Request getRequest() {
        return request;
    }

    public HttpSession getSession() {
        return session;
    }

    @Override
    public String toString() {
        return "RequestDTO{" +
                "request=" + request +
                ", session=" + session +
                '}';
    }
}
