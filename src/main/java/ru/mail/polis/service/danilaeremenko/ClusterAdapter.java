package ru.mail.polis.service.danilaeremenko;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClusterAdapter extends HttpClient {
    final String ip;
    final int port;
    private static final Logger CLUSTER_LOGGER = LoggerFactory.getLogger(BasicService.class);

    public ClusterAdapter(String ip, int port) {
        super(new ConnectionString(ip + ":" + port));
        this.ip = ip;
        this.port = port;
    }

    public static ClusterAdapter fromStringDesc(String adapterDesc) {
        String[] descList = adapterDesc.split(":");
        int port = Integer.parseInt(descList[2]);
        String ip = descList[0] + ":" + descList[1];

        return new ClusterAdapter(ip, port);
    }

    public Response processRequest(Request request) throws InterruptedException {
        final Response response;
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    response = this.get(this.toURL(), request.getHeaders());
                    break;
                case Request.METHOD_PUT:
                    response = this.put(this.toURL(), request.getBody(), request.getHeaders());
                    break;
                case Request.METHOD_DELETE:
                    response = this.delete(this.toURL(), request.getHeaders());
                    break;
                default:
                    response = new Response(
                            Response.BAD_REQUEST,
                            "Undefined method".getBytes(StandardCharsets.UTF_8)
                    );
                    break;

            }
            return response;
        } catch (IOException e) {
            String message = "IOException caught while awaiting response from server";
            CLUSTER_LOGGER.error(message, e);
            return new Response(
                    Response.BAD_REQUEST,
                    message.getBytes(StandardCharsets.UTF_8)
            );
        } catch (HttpException e) {
            String message = "HttpException caught while awaiting response from server";
            CLUSTER_LOGGER.error(message, e);
            return new Response(
                    Response.BAD_REQUEST,
                    message.getBytes(StandardCharsets.UTF_8)
            );
        } catch (PoolException e) {
            String message = "PoolException caught while awaiting response from server";
            CLUSTER_LOGGER.error(message, e);
            return new Response(
                    Response.BAD_REQUEST,
                    message.getBytes(StandardCharsets.UTF_8)
            );
        } catch (InterruptedException e) {
            String message = "Interrupted caught while awaiting response from server";
            CLUSTER_LOGGER.error(message, e);
            throw e;
        } catch (NullPointerException e) {
            //TODO one.nio.http.HttpClient methods throw NullPointerException when I pass request.getHeaders() to them
            String message = "Null pointer caught while awaiting response from server";
            CLUSTER_LOGGER.error(message, e);
            return new Response(
                    Response.BAD_REQUEST,
                    message.getBytes(StandardCharsets.UTF_8)
            );
        }

    }

    public String toURL() {
        return this.ip + ":" + this.port;
    }

    @Override
    public String toString() {
        return "ClusterAdapter{"
                + "ip='" + ip + '\''
                + ", port=" + port
                + '}';
    }

    Response processRequest() {
        return Response.ok("So, I'm dummy stub");
    }

}
