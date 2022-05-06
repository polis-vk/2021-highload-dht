package ru.mail.polis.service.alexander_kuptsov;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HttpServiceUtils {
    private HttpServiceUtils() {
    }

    /**
     * Creates an instance of {@link HttpServerConfig} by given port number.
     *
     * @param port port number
     * @return An instance of {@link HttpServerConfig}
     */
    public static HttpServerConfig createConfigByPort(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
    }

    /**
     * Extracts bytes from given buffer.
     *
     * @param buffer {@link ByteBuffer}
     * @return The new array of bytes
     **/
    public static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /**
     * Wraps a given string id into a buffer.
     *
     * @param id string id
     * @return The new byte buffer
     */
    public static ByteBuffer wrapIdToBuffer(String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }
}
