package ru.mail.polis.sharding;

import javax.annotation.Nonnull;

public interface HashRouter<T extends Node> {
    T route(@Nonnull String key);
}
