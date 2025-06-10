package com.cmex.bolt.dto;

import lombok.Builder;
import lombok.Data;

import java.util.TreeMap;

@Data
@Builder
public class DepthDto {

    private String symbol;

    private TreeMap<String, String> asks;

    private TreeMap<String, String> bids;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(symbol + " level2:\n");
        for (String key : asks.descendingMap().keySet()) {
            sb.append("| ").append(String.format("%-10s", key)).append(": ")
                    .append(String.format("%-10s", asks.get(key))).append("\n");
        }
        sb.append(".......................\n");
        for (String key : bids.descendingMap().keySet()) {
            sb.append("| ").append(String.format("%-10s", key)).append(": ")
                    .append(String.format("%-10s", bids.get(key))).append("\n");
        }
        return sb.toString();
    }
}
