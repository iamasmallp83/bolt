package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Symbol {
    private short id;

    private String name;

    private Currency base;

    private Currency quote;

    public static Symbol getSymbol(short id) {
        switch (id) {
            case 1:
                return Symbol.builder().id((short) 2).name("BTCUSDT").base(Currency.builder().id((short) 2).build())
                        .quote(Currency.builder().id((short) 1).build()).build();
            case 2:
                return Symbol.builder().id((short) 3).name("ETHUSDT").base(Currency.builder().id((short) 3).build())
                        .quote(Currency.builder().id((short) 1).build()).build();
        }
        return null;
    }

}