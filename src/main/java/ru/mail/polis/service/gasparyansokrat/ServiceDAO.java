package ru.mail.polis.service.gasparyansokrat;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class ServiceDAO {

    DAO refDao;
    LoadingCache<ByteBuffer, Record> cache; // LRU cache for store request =)
    static final int LIMIT_SIZE = 8 * 1024 * 1024; // 8 mb

    ServiceDAO(final int cacheCapacity, DAO dao) {
        int cacheSize = cacheCapacity;
        this.refDao = dao;

        if (cacheSize > LIMIT_SIZE) {
            cacheSize = LIMIT_SIZE;
        }

        cache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .build(new CacheLoader<ByteBuffer, Record>() {
                    @Override
                    public Record load(ByteBuffer id) { // no checked exception
                        Iterator<Record> it = refDao.range(id, DAO.nextKey(id));
                        if (!it.hasNext()) {
                            return Record.tombstone(id);
                        }
                        return it.next();
                    }
                });
    }

    private void updateCache(ByteBuffer id, Record rec) {
        cache.put(id, rec);
    }

    private Record checkCache(ByteBuffer id) throws IOException {
        Record data = null;
        try {
            data = cache.get(id);
        } catch (ExecutionException e) {
            throw new IOException("Bad access cache data", e);
        }
        return data;
    }

    private byte[] cvtByteArray2Bytes(final ByteBuffer bf) {
        byte[] tmpBuff = new byte[bf.remaining()];
        bf.get(tmpBuff);
        return tmpBuff;
    }

    protected Response get(final String id) throws IOException {
        ByteBuffer start = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Response resp = null;
        Record res = null;

        try {
            res = checkCache(start);
            boolean accept = false;
            if (res.isTombstone()) {
                Iterator<Record> it = refDao.range(start, null); // DAO.nextKey(start)

                while (it.hasNext() && !accept) {
                    res = it.next();
                    accept = res.getKey().equals(start);
                }

            } else {
                accept = true;
            }

            if (accept) {
                resp = new Response(Response.OK, cvtByteArray2Bytes(res.getValue()));
            } else {
                resp = new Response(Response.NOT_FOUND, Response.EMPTY);
            }

        } catch (IOException e) {
            throw new IOException("Bad value", e);
        }
        return resp;
    }

    protected Response put(final String id, final byte[] data) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        ByteBuffer payload = ByteBuffer.wrap(data);
        Record temp = Record.of(key, payload);
        refDao.upsert(temp);
        updateCache(key, temp);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    protected Response delete(final String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Record temp = Record.tombstone(key);
        refDao.upsert(temp);
        updateCache(key, temp);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    /**
     *
     * @param req
     * @param id
     * @return
     * @throws IOException
     */
    public Response handleRequest(@NotNull Request req, final String id) throws IOException {

        Response resp = null;
        try {
            final int typeHttpMethod = req.getMethod();
            switch (typeHttpMethod) {
                case Request.METHOD_GET:
                    resp = this.get(id);
                    break;
                case Request.METHOD_PUT:
                    resp = this.put(id, req.getBody());
                    break;
                case Request.METHOD_DELETE:
                    resp = this.delete(id);
                    break;
                default:
                    resp = new Response(Response.METHOD_NOT_ALLOWED, "Bad request".getBytes(StandardCharsets.UTF_8));
                    break;
            }
        } catch (IOException e) {
            throw new IOException("Error access DAO", e);
        }
        return resp;
    }
}
