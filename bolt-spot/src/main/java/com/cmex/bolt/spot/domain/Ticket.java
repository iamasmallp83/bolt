package com.cmex.bolt.spot.domain;

public class Ticket {

    private long id;
    private Order taker;
    private Order maker;
    private long price;
    private long quantity;
    private long volume;
    private Order.OrderSide takerSide;

    public Ticket() {
        // Default constructor
    }

    /**
     * Initialize ticket with values
     */
    public void init(Order taker, Order maker, long price, long quantity, long volume, Order.OrderSide takerSide) {
        this.taker = taker;
        this.maker = maker;
        this.price = price;
        this.quantity = quantity;
        this.volume = volume;
        this.takerSide = takerSide;
    }

    /**
     * Reset ticket for object pooling
     */
    public void reset() {
        this.id = 0;
        this.taker = null;
        this.maker = null;
        this.price = 0;
        this.quantity = 0;
        this.volume = 0;
        this.takerSide = null;
    }

    // Getter and setter methods
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Order getTaker() {
        return taker;
    }

    public void setTaker(Order taker) {
        this.taker = taker;
    }

    public Order getMaker() {
        return maker;
    }

    public void setMaker(Order maker) {
        this.maker = maker;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public Order.OrderSide getTakerSide() {
        return takerSide;
    }

    public void setTakerSide(Order.OrderSide takerSide) {
        this.takerSide = takerSide;
    }

    @Override
    public String toString() {
        return "Ticket{" +
                "id=" + id +
                ", price=" + price +
                ", quantity=" + quantity +
                ", volume=" + volume +
                ", takerSide=" + takerSide +
                '}';
    }
}
