package ru.mail.polis.sharding;

import javax.annotation.Nonnull;

class VirtualNode implements Node {
    public final Node node;
    public final int id;

    protected VirtualNode(@Nonnull Node node, int id) {
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
