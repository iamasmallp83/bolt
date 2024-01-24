package com.cmex.bolt.spot.util;


import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.function.Consumer;

public class FakeStreamObserver<T> implements StreamObserver<T> {

    private final Consumer<T> consumer;

    public FakeStreamObserver(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onNext(T t) {
        if (consumer != null) {
            consumer.accept(t);
        }
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {
    }

    public static <T> FakeStreamObserver<T> of(Consumer<T> consumer) {
        return new FakeStreamObserver<T>(consumer);
    }

    public static <T> FakeStreamObserver<T> noop() {
        return new FakeStreamObserver<T>(null);
    }

    public static <T> FakeStreamObserver<T> logger() {
        return new FakeStreamObserver<T>(new Consumer<T>() {
            @Override
            public void accept(T t) {
                System.out.println(Instant.now().toString() + " " + Thread.currentThread() + " : " + t);
            }
        });
    }
}