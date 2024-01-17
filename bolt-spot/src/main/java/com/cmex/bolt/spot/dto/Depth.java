package com.cmex.bolt.spot.dto;

import lombok.Builder;
import lombok.Data;

import java.util.TreeMap;

@Data
@Builder
public class Depth {

    private String symbol;

    private TreeMap<String, String> bids;

    private TreeMap<String, String> asks;

}
