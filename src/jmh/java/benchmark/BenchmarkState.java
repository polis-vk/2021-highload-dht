package benchmark;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.AnchorAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.JumpAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MaglevAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.MultiProbeAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.RendezvousAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.VNodesAlgorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.Djb2Algorithm;
import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.HashSet;
import java.util.Set;

@State(Scope.Benchmark)
public class BenchmarkState {
    @SuppressWarnings("unused")
    @Param({"4", "16", "64", "256", "1024"})
    private int topologySize;

    public IDistributionAlgorithm anchorAlgorithm;
    public IDistributionAlgorithm jumpAlgorithm;
    public IDistributionAlgorithm maglevAlgorithm;
    public IDistributionAlgorithm multiProbeAlgorithm;
    public IDistributionAlgorithm rendezvousAlgorithm;
    public IDistributionAlgorithm vNodesAlgorithm;

    public Set<String> topology;

    private static final IHashAlgorithm DEFAULT_HASH_ALGORITHM = new Djb2Algorithm();

    @Setup
    public void setup() {
        IHashAlgorithm hashAlgorithm = DEFAULT_HASH_ALGORITHM;
        anchorAlgorithm = new AnchorAlgorithm(hashAlgorithm);
        jumpAlgorithm = new JumpAlgorithm(hashAlgorithm);
        maglevAlgorithm = new MaglevAlgorithm(hashAlgorithm);
        multiProbeAlgorithm = new MultiProbeAlgorithm(hashAlgorithm);
        rendezvousAlgorithm = new RendezvousAlgorithm(hashAlgorithm);
        vNodesAlgorithm = new VNodesAlgorithm(hashAlgorithm);

        topology = new HashSet<>(BenchmarkUtils.getRandomNodes(topologySize));
        for (IDistributionAlgorithm algorithm : new IDistributionAlgorithm[]{
                anchorAlgorithm,
                jumpAlgorithm,
                maglevAlgorithm,
                multiProbeAlgorithm,
                rendezvousAlgorithm,
                vNodesAlgorithm}) {
            algorithm.addTopology(topology);
        }
    }
}
