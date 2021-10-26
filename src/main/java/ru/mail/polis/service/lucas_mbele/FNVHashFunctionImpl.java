package ru.mail.polis.service.lucas_mbele;

public class FNVHashFunctionImpl implements FNVHashFunction {

    private static final int FNV_32_INIT = 0x811c9dc5;
    private static final int FNV_32_PRIME = 0x01000193;
    private static final long FNV_64_INIT = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    public FNVHashFunctionImpl(){

    }

    @Override
    public int hash32(final byte[] key) {
        int resultValue = FNV_32_INIT;
        for (byte b : key) {
            resultValue ^= b;
            resultValue *= FNV_32_PRIME;
        }
        return resultValue;
    }

    @Override
    public long hash64(byte[] key) {
        long resultValue = FNV_64_INIT;
        for (byte b : key) {
            resultValue ^= b;
            resultValue *= FNV_64_PRIME;
        }
        return resultValue;
    }
}
