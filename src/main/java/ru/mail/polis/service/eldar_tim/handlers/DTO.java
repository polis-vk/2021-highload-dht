package ru.mail.polis.service.eldar_tim.handlers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

public class DTO implements Serializable {
    public final byte[] value;
    public final long timestamp;

    // It's OK only for non-5xx answers.
    public transient final boolean isOk;
    public transient final String responseCode;
    public transient final String errorMessage;

    private DTO(@Nonnull String responseCode, @Nullable String errorMessage) {
        this.value = null;
        this.timestamp = -1;

        this.isOk = false;
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    private DTO(@Nonnull String responseCode, @Nullable byte[] value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;

        this.isOk = true;
        this.responseCode = responseCode;
        this.errorMessage = null;
    }

    public static DTO serverError(@Nonnull String responseCode, @Nullable String errorMessage) {
        return new DTO(responseCode, errorMessage);
    }

    public static DTO answer(@Nonnull String responseCode, @Nullable byte[] value, long timestamp) {
        return new DTO(responseCode, value, timestamp);
    }

    public static DTO answer(@Nonnull String responseCode, @Nullable byte[] value) {
        return answer(responseCode, value, -1);
    }

    public static DTO answer(@Nonnull String responseCode) {
        return answer(responseCode, null, -1);
    }
}
