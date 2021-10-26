import hash.Sdbm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.mail.polis.ClusterPartitioner;
import ru.mail.polis.ClusterService;

import java.util.Set;

public class TestPartitioner {



    @Test
    public void testPartitioner() {
        ClusterService clusterService = new ClusterService(ClusterPartitioner.create(Set.of("123", "1234")), new Sdbm());
        String keyPrefix = "key";
        for (int i = 0; i < 100000; i++) {
           clusterService.getNodeForKey(keyPrefix + i);
        }

    }

}
