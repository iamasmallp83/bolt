package com.cmex.bolt.util;


import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.EnvoyServer;


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
                .build(), FakeStreamObserver.logger());
    }


    public static void getDepth(EnvoyServer service, int symbolId) {
        service.getDepth(Envoy.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }

    public static void getDepth(EnvoyServer service, int symbolId, FakeStreamObserver<Envoy.GetDepthResponse> observer) {
        service.getDepth(Envoy.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), observer);
    }

    public static void placeOrder(EnvoyServer service, long requestId, int symbolId, int accountId,
                                  Envoy.Type type, Envoy.Side side, String price, String quantity,
                                  FakeStreamObserver<Envoy.PlaceOrderResponse> observer) {
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId)
                .setSymbolId(symbolId)
                .setAccountId(accountId)
                .setType(type)
                .setSide(side)
                .setPrice(price)
                .setQuantity(quantity)
                .build(), observer);
    }

    public static void placeOrder(EnvoyServer service, long requestId, int symbolId, int accountId,
                                  Envoy.Type type, Envoy.Side side, String price, String quantity) {
        placeOrder(service, requestId, symbolId, accountId, type, side, price, quantity, FakeStreamObserver.noop());
    }

    public static void placeOrder(EnvoyServer service, long requestId, int symbolId, int accountId,
                                  Envoy.Type type, Envoy.Side side, String price, String quantity,
                                  int takerRate, int makerRate, FakeStreamObserver<Envoy.PlaceOrderResponse> observer) {
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(requestId)
                .setSymbolId(symbolId)
                .setAccountId(accountId)
                .setType(type)
                .setSide(side)
                .setPrice(price)
                .setQuantity(quantity)
                .setTakerRate(takerRate)
                .setMakerRate(makerRate)
                .build(), observer);
    }

    public static void placeOrder(EnvoyServer service, long requestId, int symbolId, int accountId,
                                  Envoy.Type type, Envoy.Side side, String price, String quantity,
                                  int takerRate, int makerRate) {
        placeOrder(service, requestId, symbolId, accountId, type, side, price, quantity, takerRate, makerRate, FakeStreamObserver.noop());
    }

    public static void cancelOrder(EnvoyServer service, long orderId) {
        service.cancelOrder(Envoy.CancelOrderRequest.newBuilder().setOrderId(orderId).build(), FakeStreamObserver.noop());
    }

    public static void cancelOrder(EnvoyServer service, long orderId, FakeStreamObserver<Envoy.CancelOrderResponse> observer) {
        service.cancelOrder(Envoy.CancelOrderRequest.newBuilder().setOrderId(orderId).build(), observer);
    }

}
