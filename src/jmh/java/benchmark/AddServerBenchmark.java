package benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import ru.mail.polis.service.alexander_kuptsov.sharding.distribution.IDistributionAlgorithm;

import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgs = {"-Xmx128m"})
@Warmup(iterations = 3)
@State(Scope.Benchmark)
@Measurement(iterations = 15)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class AddServerBenchmark {
    private String serverToAdd;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(AddServerBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup(BenchmarkState state) {
        serverToAdd = BenchmarkUtils.getNewRandomNode(state.topology);
        state.topology.add(serverToAdd);
    }

    @Benchmark
    public void anchorAddServer(BenchmarkState state) {
        addServer(state.anchorAlgorithm, serverToAdd);
    }

    @Benchmark
    public void jumpAddServer(BenchmarkState state) {
        addServer(state.jumpAlgorithm, serverToAdd);
    }

    @Benchmark
    public void maglevAddServer(BenchmarkState state) {
        addServer(state.maglevAlgorithm, serverToAdd);
    }

    @Benchmark
    public void multiProbeAddServer(BenchmarkState state) {
        addServer(state.multiProbeAlgorithm, serverToAdd);
    }

    @Benchmark
    public void rendezvousAddServer(BenchmarkState state) {
        addServer(state.rendezvousAlgorithm, serverToAdd);
    }

    @Benchmark
    public void vnodesAddServer(BenchmarkState state) {
        addServer(state.vNodesAlgorithm, serverToAdd);
    }

    private void addServer(IDistributionAlgorithm algorithm, String server) {
        algorithm.addServer(server);
    }
}
