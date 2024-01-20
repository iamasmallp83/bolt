package com.cmex.bolt.spot.api;

import org.junit.jupiter.api.Test;

public class TestMessage {

    @Test
    public void testSize(){
        Message message = new Message();
        System.out.println(message.size());
        PlaceOrder placeOrder = new PlaceOrder();
        System.out.println(placeOrder.size());
        CancelOrder cancelOrder = new CancelOrder();
        System.out.println(cancelOrder.size());
        Unfreeze unfreeze = new Unfreeze();
        System.out.println(unfreeze.size());
        Unfrozen unfrozen = new Unfrozen();
        System.out.println(unfrozen.size());
    }

    @Test
    public void testTryEvent(){
        TryEvent tryEvent = new TryEvent();
        System.out.println(tryEvent.size());
    }

}
