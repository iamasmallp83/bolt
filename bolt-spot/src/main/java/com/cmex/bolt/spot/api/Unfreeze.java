package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Unfreeze extends Struct {
    public final Signed64 currencyId = new Signed64();
    public final Signed64 amount = new Signed64();
}
