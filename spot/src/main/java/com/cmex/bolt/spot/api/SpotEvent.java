package com.cmex.bolt.spot.api;

import javolution.io.Union;

public class SpotEvent extends Union {
    public final PlaceOrder asPlaceOrder = inner(new PlaceOrder());
    public final CancelOrder asCancelOrder = inner(new CancelOrder());
    public final Freeze asFreeze = inner(new Freeze());
    public final Unfreeze asUnfreeze = inner(new Unfreeze());
    public final Deposit asDeposit = inner(new Deposit());

    public final Withdraw asWithdraw = inner(new Withdraw());

}
