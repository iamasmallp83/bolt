package com.cmex.bolt.util;


import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.Envoy;
import com.cmex.bolt.util.FakeStreamObserver;

import java.util.Map;

public class EnvoyUtil {
    public static void increase(EnvoyServer service, int accountId, int currencyId, String amount,
                                FakeStreamObserver<Envoy.IncreaseResponse> observer) {
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setAccountId(accountId)
                .setCurrencyId(currencyId)
                .setAmount(amount)
                .build(), observer);
    }

    public static void increase(EnvoyServer service, int accountId, int currencyId, String amount) {
        increase(service, accountId, currencyId, amount, FakeStreamObserver.noop());
    }

    public static void getAccount(EnvoyServer service, int accountId,
                                  FakeStreamObserver<Envoy.GetAccountResponse> observer) {
        service.getAccount(Envoy.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), observer);
    }

    public static void getAccount(EnvoyServer service, int accountId) {
        service.getAccount(Envoy.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), FakeStreamObserver.noop());
    }

    public static void getDepth(EnvoyServer service, int symbolId) {
        service.getDepth(Envoy.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }

    public static boolean equals(Map<Integer, Envoy.Balance> one, Map<Integer, Envoy.Balance> other) {
        return one == other || (one.size() == other.size() && one.entrySet().stream()
                .allMatch(entry -> other.containsKey(entry.getKey()) && entry.getValue().equals(other.get(entry.getKey()))));
    }
}
