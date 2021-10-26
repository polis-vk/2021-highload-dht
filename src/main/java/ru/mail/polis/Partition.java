package ru.mail.polis;

public class Partition {

    private final int from;
    private final int to;

    public Partition(int from, int to) {
        this.from = from;
        this.to = to;
    }

    boolean inRange(int keyHashedValue) {
        return keyHashedValue >= from && keyHashedValue <= to;
    }

}
