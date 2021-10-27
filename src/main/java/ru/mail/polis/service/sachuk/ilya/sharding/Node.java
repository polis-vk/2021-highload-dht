package ru.mail.polis.service.sachuk.ilya.sharding;

public class Node {
    public static final String HOST = "http://localhost:";
    public final int port;

    public Node(int port) {
        this.port = port;
    }
}
