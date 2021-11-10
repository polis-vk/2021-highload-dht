package ru.mail.polis.service.sachuk.ilya.replication;

import java.util.Arrays;

public final class ReplicationInfo {
    public final int ask;
    public final int from;

    private ReplicationInfo(int ask, int from) {
        this.ask = ask;
        this.from = from;
    }

    public static ReplicationInfo of(String replicas) {
        int[] arr = Arrays.stream(replicas.split("/")).mapToInt(Integer::parseInt).toArray();

        return new ReplicationInfo(arr[0], arr[1]);
    }

    public static ReplicationInfo of(int nodeNumber) {
        int ask = (nodeNumber / 2) + 1;

        return new ReplicationInfo(ask, nodeNumber);
    }
}
