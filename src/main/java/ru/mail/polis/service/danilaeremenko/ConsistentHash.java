package ru.mail.polis.service.danilaeremenko;

public class ConsistentHash {
    private final int hostNum;

    public ConsistentHash(int hostNum) {
        this.hostNum = hostNum;
    }

    public int getClusterId(String recordKey) {//TODO update implementation
        return recordKey.hashCode() % hostNum;
    }
}
