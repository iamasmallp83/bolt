package com.cmex.bolt.server.util;


import com.cmex.bolt.server.grpc.SpotServiceImpl;
import com.cmex.bolt.server.grpc.Bolt;

import java.util.Map;

public class SpotServiceUtil {
    public static void increase(SpotServiceImpl service, int accountId, int currencyId, String amount,
                                FakeStreamObserver<Bolt.IncreaseResponse> observer) {
        service.increase(Bolt.IncreaseRequest.newBuilder()
                .setAccountId(accountId)
                .setCurrencyId(currencyId)
                .setAmount(amount)
                .build(), observer);
    }

    public static void increase(SpotServiceImpl service, int accountId, int currencyId, String amount) {
        increase(service, accountId, currencyId, amount, FakeStreamObserver.noop());
    }

    public static void getAccount(SpotServiceImpl service, int accountId,
                                  FakeStreamObserver<Bolt.GetAccountResponse> observer) {
        service.getAccount(Bolt.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), observer);
    }

    public static void getAccount(SpotServiceImpl service, int accountId) {
        service.getAccount(Bolt.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), FakeStreamObserver.noop());
    }

    public static void getDepth(SpotServiceImpl service, int symbolId) {
        service.getDepth(Bolt.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }

    public static boolean equals(Map<Integer, Bolt.Balance> one, Map<Integer, Bolt.Balance> other) {
        return one == other || (one.size() == other.size() && one.entrySet().stream()
                .allMatch(entry -> other.containsKey(entry.getKey()) && entry.getValue().equals(other.get(entry.getKey()))));
    }
}
