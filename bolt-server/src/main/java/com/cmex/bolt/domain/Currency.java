package com.cmex.bolt.domain;

import lombok.Builder;
import lombok.Data;

@Data
public class Currency {
    private int id;

    private String name;

    private int precision;

    @Builder
    public Currency(int id, String name, int precision) {
        this.id = id;
        this.name = name;
        this.precision = precision;
    }

}
