package com.cmex.bolt.spot.util;

import com.cmex.bolt.spot.api.RejectionReason;

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
