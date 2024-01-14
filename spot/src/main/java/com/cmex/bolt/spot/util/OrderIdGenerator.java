package com.cmex.bolt.spot.util;

import java.time.Instant;

public class OrderIdGenerator {
    private static final int UNUSED_BITS = 1; // Sign bit, Unused (always set to 0)
    private static final int EPOCH_BITS = 39;
    private static final int SYMBOL_BITS = 11;
    private static final int SEQUENCE_BITS = 13;

    private static final long maxNodeId = (1L << SYMBOL_BITS) - 1;
    private static final long maxSequence = (1L << SEQUENCE_BITS) - 1;

    // Custom Epoch (May 14, 2015 UTC = 2020-05-14T11:31:00Z)
    private static final long DEFAULT_CUSTOM_EPOCH = 1589455860000L;

    private final long customEpoch = DEFAULT_CUSTOM_EPOCH;

    private volatile long lastTimestamp = -1L;
    private volatile long sequence = 0L;

    // Create Snowflake with a nodeId and custom epoch
    public OrderIdGenerator() {
    }

    public synchronized long nextId(long symbolId) {
        long currentTimestamp = timestamp();

        if (currentTimestamp < lastTimestamp) {
            throw new IllegalStateException("Invalid System Clock!");
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & maxSequence;
            if (sequence == 0) {
                // Sequence Exhausted, wait till next millisecond.
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0;
        }

        lastTimestamp = currentTimestamp;

        long id = currentTimestamp << (SYMBOL_BITS + SEQUENCE_BITS)
                | (symbolId << SEQUENCE_BITS)
                | sequence;

        return id;
    }


    // Get current timestamp in milliseconds, adjust for the custom epoch.
    private long timestamp() {
        return Instant.now().toEpochMilli() - customEpoch;
    }

    // Block and wait till next millisecond
    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestamp();
        }
        return currentTimestamp;
    }

    public long[] parse(long id) {
        long maskSymbolId = ((1L << SYMBOL_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;

        long timestamp = (id >> (SYMBOL_BITS + SEQUENCE_BITS)) + customEpoch;
        long symbolId = (id & maskSymbolId) >> SEQUENCE_BITS;
        long sequence = id & maskSequence;

        return new long[]{timestamp, symbolId, sequence};
    }

    public long getSymbolId(long id) {
        long maskSymbolId = ((1L << SYMBOL_BITS) - 1) << SEQUENCE_BITS;
        return (id & maskSymbolId) >> SEQUENCE_BITS;
    }

    @Override
    public String toString() {
        return "Snowflake Settings [EPOCH_BITS=" + EPOCH_BITS + ", SYMBOL_BITS=" + SYMBOL_BITS
                + ", SEQUENCE_BITS=" + SEQUENCE_BITS + ", CUSTOM_EPOCH=" + customEpoch + "]";
    }
}