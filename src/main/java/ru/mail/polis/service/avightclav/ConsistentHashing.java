package ru.mail.polis.service.avightclav;

public class ConsistentHashing {
    private final int hostNum;

    public ConsistentHashing(int hostNum) {
        this.hostNum = hostNum;
    }

    public int getClusterId(String recordKey) {
        // TODO: make it really consistent
        return Math.abs(recordKey.hashCode() % hostNum);
    }
}
