package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class ChunkedHttpSession extends HttpSession {
    private final Logger logger = LoggerFactory.getLogger(ChunkedHttpSession.class);
    private static final String FINAL_STRING_CHUNK = "0\r\n\r\n";
    private static final String NEW_LINE_STRING = "\n";
    private static final String CARETQUE_AND_NEW_LINE_STRING = "\r\n";

    private Iterator<Record> recordIterator;

    public ChunkedHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }


    public void setRecordIterator(Iterator<Record> recordIterator) {
        this.recordIterator = recordIterator;
    }

    public void sendResponseWithRange(Response response, Iterator<Record> recordIterator) throws IOException {
        this.recordIterator = recordIterator;

        response.addHeader("Transfer-Encoding: chunked");
        logger.info("before write response");

        sendResponse(response);
        logger.info("after writeREspose");

        processChain();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();

        processChain();
    }

    private void processChain() throws IOException {
        logger.info("in procces chain");
        if (recordIterator != null) {
            int offset = 0;
            logger.info("before cycle");
            while (recordIterator.hasNext() && queueHead == null) {
                Record record = recordIterator.next();

                byte[] bytes = getChunk(record.getKey(), record.getValue());

                write(bytes, 0, bytes.length);

                offset += bytes.length;

                logger.info("int while");
            }
            logger.info("after while iterator");

            if (!recordIterator.hasNext()) {
                byte[] finalChunk = getFinalChunk();
                write(finalChunk, 0, finalChunk.length);

                scheduleClose();
            }

        }

        logger.info("after write last block");
    }


    @Override
    public synchronized void scheduleClose() {
        if (recordIterator != null) {
            super.scheduleClose();
        }
    }

    private byte[] getChunk(ByteBuffer key, ByteBuffer value) {
        byte[] newLineBytes = NEW_LINE_STRING.getBytes(StandardCharsets.US_ASCII);
        byte[] caretqueAndNewLineBytes = CARETQUE_AND_NEW_LINE_STRING.getBytes(StandardCharsets.US_ASCII);

        byte[] length = Integer.toHexString(key.remaining() + newLineBytes.length + value.remaining()).getBytes(StandardCharsets.UTF_8);

        logger.info("length : {}", length.length);

        String keyString = StandardCharsets.US_ASCII.decode(key).toString();
        String valueString = StandardCharsets.US_ASCII.decode(value).toString();


        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byteArrayOutputStream.writeBytes(length);
        byteArrayOutputStream.writeBytes(caretqueAndNewLineBytes);
        byteArrayOutputStream.writeBytes(keyString.getBytes(StandardCharsets.US_ASCII));
        byteArrayOutputStream.writeBytes(newLineBytes);
        byteArrayOutputStream.writeBytes(valueString.getBytes(StandardCharsets.US_ASCII));
        byteArrayOutputStream.writeBytes(caretqueAndNewLineBytes);

        return byteArrayOutputStream.toByteArray();
    }

    private byte[] getFinalChunk() {
        return FINAL_STRING_CHUNK.getBytes(StandardCharsets.US_ASCII);
    }
}
