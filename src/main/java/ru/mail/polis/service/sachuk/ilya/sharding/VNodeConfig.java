package ru.mail.polis.service.sachuk.ilya.sharding;

public class VNodeConfig {

    private static final int NODE_WEIGHT = 5;
    public final int nodeWeight;

    public VNodeConfig() {
        this(NODE_WEIGHT);
    }

    public VNodeConfig(int nodeWeight) {
        this.nodeWeight = nodeWeight;
    }
}
