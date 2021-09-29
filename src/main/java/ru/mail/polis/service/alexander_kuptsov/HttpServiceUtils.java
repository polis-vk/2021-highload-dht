package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HttpServiceUtils {
    /**
     * Creates an instance of {@link HttpServerConfig} by given port number
     *
     * @param port port number
     * @return An instance of {@link HttpServerConfig}
     */
    public static HttpServerConfig createConfigByPort(final int port) {
        final HttpServerConfig config = new HttpServerConfig();
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    /**
     * Extracts bytes from given buffer
     *
     * @param buffer {@link ByteBuffer}
     * @return The new array of bytes
     */
    public static byte[] extractBytes(final ByteBuffer buffer) {
        final byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Wraps a given string id into a buffer
     *
     * @param id string id
     * @return The new byte buffer
     */
    public static ByteBuffer wrapIdToBuffer(String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }
}
