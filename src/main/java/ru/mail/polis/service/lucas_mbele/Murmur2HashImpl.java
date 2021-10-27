package ru.mail.polis.service.lucas_mbele;

public class Murmur2HashImpl implements Murmur2Hash 
{

    public static final int UNSIGNED_MASK = 0xff;
    public Murmur2HashImpl(){

    }
    @Override
    public int hash32(final byte[] data, int length) {
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a 'random' value
        int hash = (length & UNSIGNED_MASK);

        // Mix 4 bytes at a time into the hash
        int length4 = length >>> 2;

        for (int i = 0; i < length4; i++) {
            final int i4 = i << 2;

            int k = (data[i4] & UNSIGNED_MASK);
            k |= (data[i4 + 1] & UNSIGNED_MASK) << 8;
            k |= (data[i4 + 2] & UNSIGNED_MASK) << 16;
            k |= (data[i4 + 3] & UNSIGNED_MASK) << 24;

            k = k * m;
            k ^= k >>> r;
            k = k * m;

            hash = hash * m;
            hash = hash ^ k;
        }

        // Handle the last few bytes of the input array
        int offset = length4 << 2;
        switch (length & 3) {
            case 3:
                hash ^= ((data[offset + 2] & UNSIGNED_MASK) << 16);
                break;
            case 2:
                hash ^= ((data[offset + 1] & UNSIGNED_MASK) << 8);
                break;
            case 1:
                hash ^= (data[offset] & UNSIGNED_MASK);
                hash = hash * m;
                break;
        }

        hash ^= hash >>> 13;
        hash = hash * m;
        hash ^= hash >>> 15;

        return hash;
    }


}
