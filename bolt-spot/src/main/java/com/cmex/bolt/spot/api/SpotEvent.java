package com.cmex.bolt.spot.api;

import javolution.io.Union;

public class SpotEvent extends Union {
    public final PlaceOrder asPlaceOrder = inner(new PlaceOrder());
    public final PlaceOrderRejected asPlaceOrderRejected = inner(new PlaceOrderRejected());
    public final OrderCreated asOrderCreated = inner(new OrderCreated());
    public final Cleared asCleared = inner(new Cleared());
    public final CancelOrder asCancelOrder = inner(new CancelOrder());
    public final OrderCanceled asOrderCanceled = inner(new OrderCanceled());
    public final Freeze asFreeze = inner(new Freeze());
    public final Unfreeze asUnfreeze = inner(new Unfreeze());
    public final Increase asIncrease = inner(new Increase());
    public final Increased asIncreased = inner(new Increased());
    public final Decrease asDecrease = inner(new Decrease());
    public final Decreased asDecreased = inner(new Decreased());
    public final DecreaseRejected asDecreaseRejected = inner(new DecreaseRejected());
}
