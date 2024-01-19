package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Cleared extends Struct {
    public final Signed16 unfreezeCurrencyId = new Signed16();
    public final Signed64 unfreezeAmount = new Signed64();
    public final Signed16 currencyId = new Signed16();
    public final Signed64 amount = new Signed64();
}
