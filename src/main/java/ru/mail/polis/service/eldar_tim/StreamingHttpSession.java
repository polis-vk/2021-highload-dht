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
    private Supplier<StreamingChunk> supplier;

    public StreamingHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    public void sendStreamingResponse(Response response, Supplier<StreamingChunk> supplier) throws IOException {
        this.supplier = supplier;
        response.addHeader("Transfer-Encoding: chunked");
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
        if (supplier != null) {
            while (queueHead == null) {
                StreamingChunk chunk = supplier.get();

                if (chunk == StreamingChunk.EMPTY) {
                    byte[] bytes = StreamingChunk.EMPTY_BYTES;
                    write(bytes, 0, bytes.length);

                    super.scheduleClose();
                    return;
                } else {
                    byte[] bytes = chunk.bytes();
                    write(bytes, 0, bytes.length);
                }
            }
        }
    }
}
