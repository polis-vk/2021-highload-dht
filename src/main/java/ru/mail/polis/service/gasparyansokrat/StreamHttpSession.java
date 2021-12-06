package ru.mail.polis.service.gasparyansokrat;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;

import java.io.IOException;
import java.util.function.Supplier;

public class StreamHttpSession extends HttpSession {

    private Supplier<byte[]> dataSupplier;

    public StreamHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    public void sendResponseSupplier(Response response, Supplier<byte[]> supplier) throws IOException {
        this.dataSupplier = supplier;
        response.setBody(Response.EMPTY);
        sendResponse(response);
        processChain();
    }

    @Override
    public synchronized void scheduleClose() {
        if (dataSupplier == null) {
            super.scheduleClose();
        }
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        processChain();
    }

    private void processChain() throws IOException {
        if (dataSupplier != null) {
            while (queueHead == null) {
                byte[] data = dataSupplier.get();
                if (data == null) {
                    super.scheduleClose();
                    return;
                }
                write(data, 0, data.length);
            }
        }
    }

}
