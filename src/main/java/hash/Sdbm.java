package hash;

public final class Sdbm implements HashFunction {

    public long hash(String key) {
        long hash = 0;
        for(int i = 0; i < key.length(); ++i) {
            hash = key.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return hash;
    }

}
