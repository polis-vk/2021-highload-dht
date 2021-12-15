package ru.mail.polis.service.eldar_tim;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Reference implementation of HttpSession with streaming support.
 */
public class StreamingHttpSession extends HttpSession {
    private Supplier<byte[]> supplier;

    public StreamingHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    public void sendStreamingResponse(Response response, Supplier<byte[]> supplier) throws IOException {
        this.supplier = supplier;
        response.setBody(supplier.get());
        sendResponse(response);
        processChain();
    }

    @Override
    public synchronized void scheduleClose() {
        if (supplier == null) {
            super.scheduleClose();
        }
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        processChain();
    }

    private void processChain() throws IOException {
        if (supplier == null) {
            return;
        }

        while (queueHead == null) {
            byte[] bytes = supplier.get();
            if (bytes == null) {
//                String connection = handling.getHeader("Connection:");
//                boolean keepAlive = handling.isHttp11()
//                        ? !"close".equalsIgnoreCase(connection)
//                        : "Keep-Alive".equalsIgnoreCase(connection);
//                response.addHeader(keepAlive ? "Connection: Keep-Alive" : "Connection: close");

                // TODO: support keep alive
                super.scheduleClose();
                return;
            }
            write(bytes, 0, bytes.length);
        }
    }
}
