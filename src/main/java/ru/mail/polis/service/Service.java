package ru.mail.polis.service;

import one.nio.http.Response;

import javax.annotation.Nonnull;

public interface Service {
    String STATUS_URI_PREFIX = "/v0/status";
    String ENTITY_URI_PREFIX = "/v0/entity";

    Response status();

    Response get(@Nonnull final String id);

    Response put(@Nonnull final String id, @Nonnull byte[] body);

    Response delete(@Nonnull final String id);
}
