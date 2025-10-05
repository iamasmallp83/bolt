@0x9eb32e19f86ee174;

using Java = import "/capnp/java.capnp";
$Java.package("com.cmex.bolt");
$Java.outerClassname("Nexus");

# 基础枚举类型
enum OrderSide {
  bid @0;
  ask @1;
}

enum OrderType {
  limit @0;
  market @1;
}

enum EventType {
  snapshot @0;
  placeOrder @1;
  cancelOrder @2;
  orderCreated @3;
  orderCanceled @4;
  placeOrderRejected @5;
  cancelOrderRejected @6;
  increase @7;
  increased @8;
  increaseRejected @9;
  decrease @10;
  decreased @11;
  decreaseRejected @12;
  freeze @13;
  unfreeze @14;
  unfrozen @15;
  clear @16;
}

enum RejectionReason {
  systemBusy @0;
  currencyNotExist @1;
  accountNotFound @2;
  balanceNotEnough @3;
  symbolNotExist @4;
  orderNotExist @5;
  orderNotMatch @6;
}

# 订单相关结构
struct PlaceOrder {
  symbolId @0 :Int32;
  accountId @1 :Int32;
  type @2 :OrderType;
  side @3 :OrderSide;
  price @4 :Text;
  quantity @5 :Text;
  volume @6 :Text;
  frozen @7 :Text;
  takerRate @8 :Int32;
  makerRate @9 :Int32;
}

struct CancelOrder {
  orderId @0 :Int64;
}

struct OrderCreated {
  orderId @0 :Int64;
}

struct OrderCanceled {
  orderId @0 :Int64;
}

struct PlaceOrderRejected {
  reason @0 :RejectionReason;
}

struct CancelOrderRejected {
  reason @0 :RejectionReason;
}

# 余额相关结构
struct Increase {
  accountId @0 :Int32;
  currencyId @1 :Int32;
  amount @2 :Text;
}

struct Increased {
  currencyId @0 :Int32;
  amount @1 :Text;
  available @2 :Text;
  frozen @3 :Text;
}

struct IncreaseRejected {
  reason @0 :RejectionReason;
}

struct Decrease {
  accountId @0 :Int32;
  currencyId @1 :Int32;
  amount @2 :Text;
}

struct Decreased {
  currencyId @0 :Int32;
  amount @1 :Text;
  available @2 :Text;
  frozen @3 :Text;
}

struct DecreaseRejected {
  reason @0 :RejectionReason;
}

struct Freeze {
  accountId @0 :Int32;
  amount @1 :Text;
}

struct Unfreeze {
  accountId @0 :Int32;
  currencyId @1 :Int32;
  amount @2 :Text;
}

struct Unfrozen {
  accountId @0 :Int32;
  amount @1 :Text;
}

struct Clear {
    accountId @0 :Int32;
    payCurrencyId @1 :Int32;
    payAmount @2 :Text;
    refundAmount @3 :Text;
    incomeCurrencyId @4 :Int32;
    incomeAmount @5 :Text;
}

struct Snapshot {
  timestamp @0 :Int64;
}

struct EmptyEvent {
}

# 联合类型：NexusEvent
struct Payload {
  union {
    empty @0 :EmptyEvent;
    snapshot @1 :Snapshot;
    placeOrder @2 :PlaceOrder;
    cancelOrder @3 :CancelOrder;
    orderCreated @4 :OrderCreated;
    orderCanceled @5 :OrderCanceled;
    placeOrderRejected @6 :PlaceOrderRejected;
    cancelOrderRejected @7 :CancelOrderRejected;
    increase @8 :Increase;
    increased @9 :Increased;
    increaseRejected @10 :IncreaseRejected;
    decrease @11 :Decrease;
    decreased @12 :Decreased;
    decreaseRejected @13 :DecreaseRejected;
    freeze @14 :Freeze;
    unfreeze @15 :Unfreeze;
    unfrozen @16 :Unfrozen;
    clear @17 :Clear;
  }
}

# 顶层消息结构
struct NexusEvent {
  payload @0 :Payload;
}