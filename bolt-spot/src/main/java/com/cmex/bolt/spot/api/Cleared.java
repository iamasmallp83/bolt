package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Cleared extends Struct {
    public final Signed32 accountId = new Signed32();
    public final Signed16 payCurrencyId = new Signed16();
    public final Signed64 payAmount = new Signed64();
    public final Signed16 incomeCurrencyId = new Signed16();
    public final Signed64 incomeAmount = new Signed64();
}
