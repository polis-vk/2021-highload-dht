package ru.mail.polis.service.sachuk.ilya.sharding;

public class Node {
    public final String schema;
    public final String protocol;
    public final String host;
    public final int port;
    public final String connectionString;

    public Node(String protocol, String host, int port, String connectionString) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.connectionString = connectionString;
        this.schema = protocol + "://" + host;
    }
}

