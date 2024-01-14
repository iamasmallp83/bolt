package com.cmex.bolt.spot.api;

import javolution.io.Struct;
import lombok.Data;

public class Withdraw extends Struct {
    public final Signed32 accountId = new Signed32();

    public final Signed16 currencyId = new Signed16();
    public final Signed64 amount = new Signed64();
}
