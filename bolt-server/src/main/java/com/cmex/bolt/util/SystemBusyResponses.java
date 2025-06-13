package com.cmex.bolt.util;


import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;

/**
 * 系统繁忙响应工厂
 */
public class SystemBusyResponses {
    private static final String BUSY_MESSAGE = "System is busy, please try again later";
    private static final int BUSY_CODE = Nexus.RejectionReason.SYSTEM_BUSY.ordinal();

    public static Envoy.PlaceOrderResponse createPlaceOrderBusyResponse() {
        return Envoy.PlaceOrderResponse.newBuilder().setCode(BUSY_CODE).setMessage(BUSY_MESSAGE).build();
    }

    public static Envoy.IncreaseResponse createIncreaseBusyResponse() {
        return Envoy.IncreaseResponse.newBuilder().setCode(BUSY_CODE).setMessage(BUSY_MESSAGE).build();
    }

    public static Envoy.DecreaseResponse createDecreaseBusyResponse() {
        return Envoy.DecreaseResponse.newBuilder().setCode(BUSY_CODE).setMessage(BUSY_MESSAGE).build();
    }

    public static Envoy.CancelOrderResponse createCancelOrderBusyResponse() {
        return Envoy.CancelOrderResponse.newBuilder().setCode(BUSY_CODE).setMessage(BUSY_MESSAGE).build();
    }
}