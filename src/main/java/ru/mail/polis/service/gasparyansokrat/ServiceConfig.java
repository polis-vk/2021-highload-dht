package ru.mail.polis.service.gasparyansokrat;

public final class ServiceConfig {

    public final int port;
    public final int poolSize;
    public final String address;

    /**
     * new doc.
     */
    public ServiceConfig(final int port, final int poolSize,
                         final String address) {
        this.port = port;
        this.poolSize = poolSize;
        this.address = address;
    }
}
