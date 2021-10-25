/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.service;

/**
 * A persistent storage with HTTP API.
 *
 * <p>The following HTTP protocol is supported:
 * <ul>
 * <li>{@code GET /v0/status} -- returns {@code 200} or {@code 503}</li>
 * <li>{@code GET /v0/entity?id=<ID>} -- get data by {@code ID}.
 * Returns {@code 200} and data if found, {@code 404} if not found.</li>
 * <li>{@code PUT /v0/entity?id=<ID>} -- upsert (create or replace) data by {@code ID}. Returns {@code 201}.</li>
 * <li>{@code DELETE /v0/entity?id=<ID>} -- remove data by {@code ID}. Returns {@code 202}.</li>
 * </ul>
 *
 * <p>{@code ID} is a non empty char sequence.
 *
 * <p>In all the cases the storage may return:
 * <ul>
 * <li>{@code 4xx} for malformed requests</li>
 * <li>{@code 5xx} for internal errors</li>
 * </ul>
 *
 * @author Vadim Tsesko
 */
public interface Service {
    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>May be called only once.
     */
    void start();

    /**
     * Stop listening and free all the resources.
     *
     * <p>May be called only once and after {@link #start()}.
     */
    void stop();
}
