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
public class RemoveServerBenchmark {
    private String serverToRemove;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RemoveServerBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup(BenchmarkState state) {
        serverToRemove = state.topology.iterator().next();
        state.topology.remove(serverToRemove);
    }

    @Benchmark
    public void anchorRemoveServer(BenchmarkState state) {
        removeServer(state.anchorAlgorithm, serverToRemove);
    }

    @Benchmark
    public void jumpRemoveServer(BenchmarkState state) {
        removeServer(state.jumpAlgorithm, serverToRemove);
    }

    @Benchmark
    public void maglevRemoveServer(BenchmarkState state) {
        removeServer(state.maglevAlgorithm, serverToRemove);
    }

    @Benchmark
    public void multiProbeRemoveServer(BenchmarkState state) {
        removeServer(state.multiProbeAlgorithm, serverToRemove);
    }

    @Benchmark
    public void rendezvousRemoveServer(BenchmarkState state) {
        removeServer(state.rendezvousAlgorithm, serverToRemove);
    }

    @Benchmark
    public void vnodesRemoveServer(BenchmarkState state) {
        removeServer(state.vNodesAlgorithm, serverToRemove);
    }

    private void removeServer(IDistributionAlgorithm algorithm, String server) {
        algorithm.removeServer(server);
    }
}
