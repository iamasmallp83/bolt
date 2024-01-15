package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Freeze extends Struct{
    public final Struct.Signed32 accountId = new Struct.Signed32();
    public final Struct.Signed64 amount = new Struct.Signed64();
}
