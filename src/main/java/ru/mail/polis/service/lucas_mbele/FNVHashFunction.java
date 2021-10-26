package ru.mail.polis.service.lucas_mbele;

public interface FNVHashFunction {
    int hash32(byte[] key);
    long hash64(byte[] key);
}
