package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Currency {
    private int id;
    private String name;
}
