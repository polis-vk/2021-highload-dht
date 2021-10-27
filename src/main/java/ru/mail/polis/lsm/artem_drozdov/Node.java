package ru.mail.polis.lsm.artem_drozdov;

import one.nio.http.HttpClient;

public class Node {
    private final String path;
    private final HttpClient httpClient;

    public Node(String path, HttpClient httpClient) {
        this.path = path;
        this.httpClient = httpClient;
    }

    public String getPath() {
        return path;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
