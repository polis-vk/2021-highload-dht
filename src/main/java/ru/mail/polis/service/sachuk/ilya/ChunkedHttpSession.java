package ru.mail.polis.service.sachuk.ilya;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;

public class ChunkedHttpSession extends HttpSession {
    private Logger logger = LoggerFactory.getLogger(ChunkedHttpSession.class);
    private static final String FINAL_STRING_CHUNK = "0\r\n\r\n";
    private static final String NEW_LINE_STRING = "\n";
    private static final String CARETQUE_AND_NEW_LINE_STRING = "\r\n";

    private Iterator<Record> recordIterator;
    private int totalLength;

    public ChunkedHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }


    public void setRecordIterator(Iterator<Record> recordIterator) {
        this.recordIterator = recordIterator;
    }

    public void sendResponseWithRange(Response response, Iterator<Record> recordIterator) throws IOException {
        this.recordIterator = recordIterator;

//        Record record = recordIterator.hasNext() ? recordIterator.next() : null;
//
//        byte[] bytes = record == null ? new byte[0] : getChunk(record.getKey(), record.getValue());


//        response.addHeader("Content-Length: " + bytes.length);
//        sendResponse(response);

//        response.setBody(bytes);
//        response.addHeader("Content-Length: " + bytes.length);
//        response.setBody(new byte[0]);
        response.addHeader("Transfer-Encoding: chunked");
        logger.info("before write response");
//        sendResponse(response);

//        writeResponse(response, false);
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
        logger.info("iterator has next " + recordIterator.hasNext());
        if (recordIterator != null) {
            int offset = 0;
            logger.info("before cycle");
            while (recordIterator.hasNext()) {
                Record record = recordIterator.next();

                byte[] bytes = getChunk(record.getKey(), record.getValue());
//                currLength += bytes.length;

                
                write(bytes, 0, bytes.length);

                offset += bytes.length;

                logger.info("int while");
            }
            logger.info("after while iterator");

            if (!recordIterator.hasNext()) {
                byte[] finalChunk = getFinalChunk();
                write(finalChunk, 0, finalChunk.length);
                server.incRequestsProcessed();
//                super.scheduleClose();
                handleRequest();
            }

        }



        logger.info("after write last block");
    }

    private void handleRequest() throws IOException {
        if ((handling=pipeline.pollFirst()) != null ) {
            if (handling == FIN) {
                scheduleClose();
            }
             else {
                 server.handleRequest(handling, this);
            }
        }
    }

    @Override
    public synchronized void scheduleClose() {
        if (!recordIterator.hasNext()) {
            super.scheduleClose();
        }
    }

    private byte[] getChunk(ByteBuffer key, ByteBuffer value) {
//        int size = key.remaining() + value.remaining();

        String keyString = StandardCharsets.US_ASCII.decode(key).toString();
        String valueString = StandardCharsets.US_ASCII.decode(value).toString();


        String union = keyString + "\n" + valueString;

//        int size = key.remaining() + NEW_LINE_STRING.length() + value.remaining();

        int size = union.getBytes(StandardCharsets.US_ASCII).length;

        String finalString = size + "\r\n" + union;

        logger.info(finalString);

        return finalString.getBytes(StandardCharsets.US_ASCII);
    }

    private byte[] getFinalChunk() {
        return FINAL_STRING_CHUNK.getBytes(StandardCharsets.US_ASCII);
    }
}
