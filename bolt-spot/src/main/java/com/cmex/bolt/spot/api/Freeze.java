package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Freeze extends Struct{
    public final Signed32 accountId = new Signed32();
    public final Signed32 currencyId = new Signed32();
    public final Signed64 amount = new Signed64();
}
