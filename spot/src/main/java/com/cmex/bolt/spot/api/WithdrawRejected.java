package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class WithdrawRejected extends Struct {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());
}
