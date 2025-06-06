package com.cmex.bolt.spot.domain;

import javolution.io.Struct;
import com.cmex.bolt.spot.util.ObjectPool;

public class Ticket extends Struct implements ObjectPool.Poolable {
    
    public final Signed64 id = new Signed64();
    public final Signed64 takerId = new Signed64();
    public final Signed64 makerId = new Signed64();
    public final Signed64 price = new Signed64();
    public final Signed64 quantity = new Signed64();
    public final Signed64 volume = new Signed64();
    public final Enum32<Order.OrderSide> takerSide = new Enum32<>(Order.OrderSide.values());
    
    // 运行时引用（不序列化）
    private transient Order taker;
    private transient Order maker;
    
    // 实现Poolable接口的reset方法
    @Override
    public Ticket reset() {
        id.set(0);
        takerId.set(0);
        makerId.set(0);
        price.set(0);
        quantity.set(0);
        volume.set(0);
        takerSide.set(Order.OrderSide.BID);
        taker = null;
        maker = null;
        return this;
    }
    
    public Ticket init(Order taker, Order maker, long price, long quantity, 
                      long volume, Order.OrderSide takerSide) {
        this.taker = taker;
        this.maker = maker;
        this.takerId.set(taker.getId());
        this.makerId.set(maker.getId());
        this.price.set(price);
        this.quantity.set(quantity);
        this.volume.set(volume);
        this.takerSide.set(takerSide);
        return this;
    }
    
    // Getter方法
    public long getId() { return id.get(); }
    public Order getTaker() { return taker; }
    public Order getMaker() { return maker; }
    public long getPrice() { return price.get(); }
    public long getQuantity() { return quantity.get(); }
    public long getVolume() { return volume.get(); }
    public Order.OrderSide getTakerSide() { return takerSide.get(); }
}