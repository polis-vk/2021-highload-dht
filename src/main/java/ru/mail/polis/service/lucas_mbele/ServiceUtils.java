package ru.mail.polis.service.lucas_mbele;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import java.nio.ByteBuffer;


public final class ServiceUtils
{
    private ServiceUtils() {
        // Don't instantiate
    }
    /**
     * Creates and returns a proper config for acceptors of our service.
     */
    public static AcceptorConfig acceptors(int port)
    {
        // Minimal configuration of our service
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.deferAccept = true; // We assume a response accept with a delay
        acceptorConfig.reusePort = true;
        acceptorConfig.port = port;
        //Let's assume that default size for both of our
        //receiver and sender buffers will be 8 Kb
        //so not too big to prevent memory waste
        acceptorConfig.recvBuf = 8 * 1024;
        acceptorConfig.sendBuf = 8 * 1024;
        return acceptorConfig;
    }
    /**
     * Allow us to extract bytes from our buffer.
     */
    public static byte [] extractBytesBuffer(ByteBuffer buffer)
    {
        byte [] remaining = new byte[buffer.remaining()];
        buffer.get(remaining);
        return remaining;
    }


    }
