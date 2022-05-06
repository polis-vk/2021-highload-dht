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
public class GetServerBenchmark {
    private String randomRequestId;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GetServerBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        randomRequestId = BenchmarkUtils.randomId();
    }

    @Benchmark
    public void anchorGetServer(BenchmarkState state) {
        getServer(state.anchorAlgorithm, randomRequestId);
    }

    @Benchmark
    public void jumpGetServer(BenchmarkState state) {
        getServer(state.jumpAlgorithm, randomRequestId);
    }

    @Benchmark
    public void maglevGetServer(BenchmarkState state) {
        getServer(state.maglevAlgorithm, randomRequestId);
    }

    @Benchmark
    public void multiProbeGetServer(BenchmarkState state) {
        getServer(state.multiProbeAlgorithm, randomRequestId);
    }

    @Benchmark
    public void rendezvousGetServer(BenchmarkState state) {
        getServer(state.rendezvousAlgorithm, randomRequestId);
    }

    @Benchmark
    public void vnodesGetServer(BenchmarkState state) {
        getServer(state.vNodesAlgorithm, randomRequestId);
    }

    private void getServer(IDistributionAlgorithm algorithm, String id) {
        algorithm.getServer(id);
    }
}