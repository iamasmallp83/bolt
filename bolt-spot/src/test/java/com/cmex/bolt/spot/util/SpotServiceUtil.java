package com.cmex.bolt.spot.util;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;

public class SpotServiceUtil {
    public static void increase(SpotServiceImpl service, int accountId, int currencyId, String amount,
                                   FakeStreamObserver<SpotServiceProto.IncreaseResponse> observer) {
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setAccountId(accountId)
                .setCurrencyId(currencyId)
                .setAmount(amount)
                .build(), observer);
    }

    public static void increase(SpotServiceImpl service, int accountId, int currencyId, String amount) {
        increase(service, accountId, currencyId, amount, FakeStreamObserver.noop());
    }

    public static void getAccount(SpotServiceImpl service, int accountId,
                                     FakeStreamObserver<SpotServiceProto.GetAccountResponse> observer) {
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), observer);
    }

    public static void getAccount(SpotServiceImpl service, int accountId) {
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), FakeStreamObserver.noop());
    }

    public static void getDepth(SpotServiceImpl service, int symbolId) {
        service.getDepth(SpotServiceProto.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }
}
