package ru.mail.polis.service.igorsamokhin;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.util.ByteArrayBuilder;

import java.io.IOException;
import java.util.function.Supplier;

public class Session extends HttpSession {
    private Supplier<byte[]> supplier;
    private boolean chunked;

    public Session(Socket socket, HttpServer server) {
        super(socket, server);
    }

    public void sendChunkedResponse(Response response, Supplier<byte[]> supplier) throws IOException {
        this.supplier = supplier;
        byte[] bodyFromSupplier = supplier.get();
        byte[] initialBody = bodyFromSupplier;

        if (bodyFromSupplier == null) {
            initialBody = new byte[0];
        }
        byte[] bytes = prepareBytes(initialBody);
        response.setBody(bytes);

        this.chunked = (bodyFromSupplier != null);
        sendResponse(response);
        this.chunked = false;
    }

    private void processChain() throws IOException {
        if (supplier != null) {
            while (queueHead == null) {
                byte[] bytes = supplier.get();
                if (bytes == null) {
                    byte[] empty = prepareBytes(new byte[0]);
                    write(empty, 0, empty.length);

                    return;
                }

                bytes = prepareBytes(bytes);
                write(bytes, 0, bytes.length);
            }

        }
    }

    private byte[] prepareBytes(byte[] bytes) {
        if (bytes == null) {
            return new byte[0];
        }

        ByteArrayBuilder builder = new ByteArrayBuilder(bytes.length + 4 + Integer.BYTES);
        builder.append(Integer.toHexString(bytes.length))
                .append("\r\n")
                .append(bytes)
                .append("\r\n");

        return builder.trim();
    }

    @Override
    protected void writeResponse(Response response, boolean includeBody) throws IOException {
        super.writeResponse(response, includeBody);
        if (chunked) {
            this.processChain();
        }
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
}
