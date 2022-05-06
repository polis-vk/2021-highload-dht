package ru.mail.polis.service.alexander_kuptsov.sharding.distribution;

import ru.mail.polis.service.alexander_kuptsov.sharding.hash.IHashAlgorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class MaglevAlgorithm extends DistributionHashAlgorithm<IHashAlgorithm> {
    private int lookupSize;
    private String[] currentLookup;
    private final List<Permutation> permutations;

    private static final int RECOMMENDED_LOOKUP_SHIFT = 6;
    private static final int DEFAULT_LOOKUP_SIZE = 127;

    public MaglevAlgorithm(IHashAlgorithm hashAlgorithm) {
        this(hashAlgorithm, DEFAULT_LOOKUP_SIZE);
    }

    public MaglevAlgorithm(IHashAlgorithm hashAlgorithm, int lookupSize) {
        super(hashAlgorithm);
        this.lookupSize = lookupSize;
        this.currentLookup = new String[0];
        this.permutations = new ArrayList<>();
    }

    @Override
    public void addTopology(Set<String> topology) {
        int requiredLookupSize = topology.size() << RECOMMENDED_LOOKUP_SHIFT;
        lookupSize = lookupSize < requiredLookupSize
                ? nextPrime(requiredLookupSize)
                : lookupSize;
        permutations.forEach(Permutation::reset);
        for (String server : topology) {
            permutations.add(createPermutation(server));
        }
        currentLookup = newLookup();
    }

    @Override
    public void addServer(String server) {
        permutations.forEach(Permutation::reset);
        permutations.add(createPermutation(server));
        currentLookup = newLookup();
    }

    @Override
    public String getServer(String id) {
        int index = getHash(id) % currentLookup.length;
        return currentLookup[index];
    }

    @Override
    public void removeServer(String server) {
        Permutation permutationToRemove = null;
        for (Permutation permutation : permutations) {
            if (Objects.equals(permutation.server, server)) {
                permutationToRemove = permutation;
                break;
            }
        }
        if (permutationToRemove == null) {
            return;
        }
        permutations.remove(permutationToRemove);
        permutations.forEach(Permutation::reset);
        currentLookup = newLookup();
    }

    @Override
    public void removeTopology(Set<String> servers) {
        permutations.removeIf(permutation -> servers.contains(permutation.server));
        permutations.forEach(Permutation::reset);
        currentLookup = newLookup();
    }

    private Permutation createPermutation(String backend) {
        return new Permutation(backend, getHashAlgorithm(), lookupSize);
    }

    private String[] newLookup() {
        String[] lookup = new String[lookupSize];
        int filled = 0;

        do {
            for (Permutation permutation : permutations) {
                int pos = permutation.next();
                if (lookup[pos] == null) {
                    lookup[pos] = permutation.server();
                    if (filled++ >= lookupSize) {
                        break;
                    }
                }
            }

        } while (filled < lookupSize);

        return lookup;
    }

    public int nextPrime(int num) {
        if (num <= 2) {
            return 2;
        }
        int startNum = num;
        if (startNum % 2 == 0) {
            startNum++;
        }
        for (int currentNum = startNum; currentNum < Integer.MAX_VALUE; currentNum += 2) {
            int top = (int) Math.sqrt(currentNum);
            boolean isPrime = true;
            for (int i = 3; i < top; i += 2) {
                if (currentNum % i == 0) {
                    isPrime = false;
                    break;
                }
            }
            if (isPrime) {
                return currentNum;
            }
        }
        return Integer.MAX_VALUE;
    }

    private static class Permutation {
        private static final int OFFSET_SEED = 0xABCDDBCA;
        private static final int SKIP_SEED = 0xFF00BBAA;

        private final String server;
        private final int size;
        private final int offset;
        private final int skip;

        private int current;

        public Permutation(String server, IHashAlgorithm hashAlgorithm, int size) {
            this.size = size;
            this.server = server;
            this.offset = hashAlgorithm.getHashWithSeed(server, OFFSET_SEED) % size;
            this.skip = hashAlgorithm.getHashWithSeed(server, SKIP_SEED) % (size - 1) + 1;
            this.current = offset;
        }

        public String server() {
            return server;
        }

        public int next() {
            int last = current;
            current = (last + skip) % size;
            return last;
        }

        public void reset() {
            this.current = offset;
        }
    }
}
