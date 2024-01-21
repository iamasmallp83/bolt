package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.util.NumberUtils;
import lombok.Builder;
import lombok.Data;

@Data
public class Currency {
    private int id;
    private String name;

    private int precision;

    private long multiplier;

    @Builder
    public Currency(int id, String name, int precision) {
        this.id = id;
        this.name = name;
        this.precision = precision;
        this.multiplier = NumberUtils.powLong(precision);
    }
}
