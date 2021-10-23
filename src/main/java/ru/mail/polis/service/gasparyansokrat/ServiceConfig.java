package ru.mail.polis.service.gasparyansokrat;

public final class ServiceConfig {

    public final int port;
    public final int poolSize;
    public final String address;
    public final int clusterIntervals;

    /**
     * new doc.
     */
    public ServiceConfig(final int port, final int poolSize,
                         final String address, final int clusterIntervals) {
        this.port = port;
        this.poolSize = poolSize;
        this.address = address;
        this.clusterIntervals = clusterIntervals;
    }
}
