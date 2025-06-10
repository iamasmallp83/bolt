package com.cmex.bolt.util;

import static com.cmex.bolt.Nexus.RejectionReason;

public record Result<T>(RejectionReason reason, T value) {

    public static <T> Result<T> success(T t) {
        return new Result<>(null, t);
    }

    public static <T> Result<T> fail(RejectionReason reason) {
        return new Result<>(reason, null);
    }

    public boolean isSuccess() {
        return reason == null;
    }
}
