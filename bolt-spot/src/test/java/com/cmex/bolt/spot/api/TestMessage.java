package com.cmex.bolt.spot.api;

import org.junit.jupiter.api.Test;

public class TestMessage {

    @Test
    public void testSize(){
        Message message = new Message();
        System.out.println(message.size());
        message.type.set(EventType.PLACE_ORDER);
        message.id.set(1);
        Decrease decrease = message.payload.asDecrease;
        decrease.accountId.set((short) 1);
        decrease.currencyId.set((short) 1);
        decrease.amount.set((short) 1);
        System.out.println(message.size());
    }

    @Test
    public void testTryEvent(){
        TryEvent tryEvent = new TryEvent();
        System.out.println(tryEvent.size());
    }

}
