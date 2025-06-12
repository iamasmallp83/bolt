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
  placeOrder @0;
  cancelOrder @1;
  orderCreated @2;
  orderCanceled @3;
  placeOrderRejected @4;
  cancelOrderRejected @5;
  increase @6;
  increased @7;
  increaseRejected @8;
  decrease @9;
  decreased @10;
  decreaseRejected @11;
  freeze @12;
  unfreeze @13;
  unfrozen @14;
  clear @15;
  tryEvent @16;
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
  price @4 :Int64;
  quantity @5 :Int64;
  volume @6 :Int64;
  frozen @7 :Int64;
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
  amount @2 :Int64;
}

struct Increased {
  currencyId @0 :Int32;
  amount @1 :Int64;
  available @2 :Int64;
  frozen @3 :Int64;
}

struct IncreaseRejected {
  reason @0 :RejectionReason;
}

struct Decrease {
  accountId @0 :Int32;
  currencyId @1 :Int32;
  amount @2 :Int64;
}

struct Decreased {
  currencyId @0 :Int32;
  amount @1 :Int64;
  available @2 :Int64;
  frozen @3 :Int64;
}

struct DecreaseRejected {
  reason @0 :RejectionReason;
}

struct Freeze {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Unfreeze {
  accountId @0 :Int32;
  currencyId @1 :Int32;
  amount @2 :Int64;
}

struct Unfrozen {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Clear {
    accountId @0 :Int32;
    payCurrencyId @1 :Int32;
    payAmount @2 :Int64;
    refundAmount @3 :Int64;
    incomeCurrencyId @4 :Int32;
    incomeAmount @5 :Int64;
}

struct EmptyEvent {
}

# 联合类型：NexusEvent
struct Payload {
  union {
    empty @0 :EmptyEvent;
    placeOrder @1 :PlaceOrder;
    cancelOrder @2 :CancelOrder;
    orderCreated @3 :OrderCreated;
    orderCanceled @4 :OrderCanceled;
    placeOrderRejected @5 :PlaceOrderRejected;
    cancelOrderRejected @6 :CancelOrderRejected;
    increase @7 :Increase;
    increased @8 :Increased;
    increaseRejected @9 :IncreaseRejected;
    decrease @10 :Decrease;
    decreased @11 :Decreased;
    decreaseRejected @12 :DecreaseRejected;
    freeze @13 :Freeze;
    unfreeze @14 :Unfreeze;
    unfrozen @15 :Unfrozen;
    clear @16 :Clear;
  }
}

# 顶层消息结构
struct NexusEvent {
  payload @0 :Payload;
}