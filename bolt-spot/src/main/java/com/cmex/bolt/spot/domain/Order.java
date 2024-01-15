package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Order {

    private long id;

    private int accountId;

    private boolean buy;

    private long price;

    private long size;

}
