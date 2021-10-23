package ru.mail.polis.service.sharding;

import javax.annotation.Nonnull;

class VirtualNode<T extends Node> implements Node {
    public final T node;
    public final int id;

    protected VirtualNode(@Nonnull T node, int id) {
        this.node = node;
        this.id = id;
    }

    public boolean isWorkerFor(@Nonnull Node node) {
        return this.node.getKey().equals(node.getKey());
    }

    @Override
    public String getKey() {
        return node.getKey() + "::" + id;
    }
}
