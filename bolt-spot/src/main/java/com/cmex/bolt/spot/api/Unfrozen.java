package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class Unfrozen extends Struct {
    public final Signed64 freezeId = new Signed64();
    public final Signed64 amount = new Signed64();
}
