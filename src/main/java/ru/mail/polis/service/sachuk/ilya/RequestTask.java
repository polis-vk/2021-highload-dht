package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.Request;
import one.nio.net.Session;

public class RequestTask {
    public final Session session;
    public final Request request;

    public RequestTask(Request request, Session session) {
        this.session = session;
        this.request = request;
    }
}
