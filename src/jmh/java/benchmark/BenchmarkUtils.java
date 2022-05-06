package benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class BenchmarkUtils {
    private static final int MIN_ALLOWED_PORT = 1;
    private static final int MAX_ALLOWED_PORT = 65535;

    public static String getNewRandomNode(Set<String> topology) {
        String node;
        int port;
        do {
            port = randomPort();
            node = endpoint(port);
        } while (topology.contains(node));
        return node;
    }

    public static List<String> getRandomNodes(final int size) {
        List<Integer> ports = new ArrayList<>(size);
        int port;
        for (int i = 0; i < size; i++) {
            do {
                port = randomPort();
            } while (ports.contains(port));
            ports.add(port);
        }
        return ports
                .stream()
                .map(BenchmarkUtils::endpoint)
                .collect(Collectors.toList());
    }

    public static int randomPort() {
        return MIN_ALLOWED_PORT + (int) (Math.random() * (MAX_ALLOWED_PORT - MIN_ALLOWED_PORT));
    }

    public static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    public static String endpoint(final int port) {
        return "http://localhost:" + port;
    }
}
