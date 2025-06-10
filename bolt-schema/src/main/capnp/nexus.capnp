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
  cleared @15;
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
  symbolId @1 :Int32;
  accountId @2 :Int32;
  type @3 :OrderType;
  side @4 :OrderSide;
  price @5 :Int64;
  quantity @6 :Int64;
  volume @7 :Int64;
  frozen @8 :Int64;
  takerRate @9 :Int32;
  makerRate @10 :Int32;
}

struct OrderCanceled {
  orderId @0 :Int64;
  symbolId @1 :Int32;
  accountId @2 :Int32;
  unfrozen @3 :Int64;
}

struct PlaceOrderRejected {
  symbolId @0 :Int32;
  accountId @1 :Int32;
  reason @2 :RejectionReason;
  message @3 :Text;
}

struct CancelOrderRejected {
  orderId @0 :Int64;
  accountId @1 :Int32;
  reason @2 :RejectionReason;
  message @3 :Text;
}

# 余额相关结构
struct Increase {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Increased {
  accountId @0 :Int32;
  amount @1 :Int64;
  newBalance @2 :Int64;
  availableBalance @3 :Int64;
  frozenBalance @4 :Int64;
  version @5 :Int64;
}

struct IncreaseRejected {
  accountId @0 :Int32;
  amount @1 :Int64;
  reason @2 :RejectionReason;
  message @3 :Text;
}

struct Decrease {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Decreased {
  accountId @0 :Int32;
  amount @1 :Int64;
  newBalance @2 :Int64;
  availableBalance @3 :Int64;
  frozenBalance @4 :Int64;
  version @5 :Int64;
}

struct DecreaseRejected {
  accountId @0 :Int32;
  amount @1 :Int64;
  reason @2 :RejectionReason;
  message @3 :Text;
}

struct Freeze {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Unfreeze {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Unfrozen {
  accountId @0 :Int32;
  amount @1 :Int64;
}

struct Cleared {
  accountId @0 :Int32;
  availableBalance @1 :Int64;
  frozenBalance @2 :Int64;
  version @3 :Int64;
}

struct TryEvent {
  # Empty for now
}

# 联合类型：NexusEvent
struct Payload {
  union {
    placeOrder @0 :PlaceOrder;
    cancelOrder @1 :CancelOrder;
    orderCreated @2 :OrderCreated;
    orderCanceled @3 :OrderCanceled;
    placeOrderRejected @4 :PlaceOrderRejected;
    cancelOrderRejected @5 :CancelOrderRejected;
    increase @6 :Increase;
    increased @7 :Increased;
    increaseRejected @8 :IncreaseRejected;
    decrease @9 :Decrease;
    decreased @10 :Decreased;
    decreaseRejected @11 :DecreaseRejected;
    freeze @12 :Freeze;
    unfreeze @13 :Unfreeze;
    unfrozen @14 :Unfrozen;
    cleared @15 :Cleared;
    tryEvent @16 :TryEvent;
  }
}

# 顶层消息结构
struct NexusEvent {
  type @0 :EventType;
  id @1 :Int64;
  payload @2 :Payload;
}