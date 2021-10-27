package ru.mail.polis.service.danilaeremenko;

public class ConsistentHash {
    private final int hostNum;

    public ConsistentHash(int hostNum) {
        this.hostNum = hostNum;
    }

    //TODO update implementation (is a stub, I swear, I will implement another methods!)
    public int getClusterId(String recordKey) {
        return recordKey.hashCode() % hostNum;
    }
}
