package ru.mail.polis.service.sachuk.ilya.sharding;

public class VNode {
    private final Node physicalNode;

    public VNode(Node physicalNode) {
        this.physicalNode = physicalNode;
    }
}
