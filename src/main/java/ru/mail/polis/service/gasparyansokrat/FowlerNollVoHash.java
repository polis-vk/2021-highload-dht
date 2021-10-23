package ru.mail.polis.service.gasparyansokrat;

public final class FowlerNollVoHash {

    private static final int FNV_MAGIC = 0x811c9dc5;
    private static final int FNV_PRIME = 0x01000193;

    /**
     * some docus.
     */
    FowlerNollVoHash() {

    }

    public int hash(final byte[] k) {
        int hashVal = FNV_MAGIC;
        final int len = k.length;
        for (int i = 0; i < len; i++) {
            hashVal ^= k[i];
            hashVal *= FNV_PRIME;
        }
        return hashVal;
    }

}
