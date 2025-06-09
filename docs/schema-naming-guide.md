# Bolt 交易系统 Schema 命名规范指南

## 📌 设计理念

**Bolt** = 闪电般的响应速度，体现在命名中强调：
- **性能导向**：命名体现高频交易特性
- **领域驱动**：按业务领域分离关注点
- **版本友好**：便于后续扩展和维护

## 🗂️ 文件组织结构

```
bolt-schema/
├── src/main/
│   ├── capnp/                     # Cap'n Proto 事件定义（内部高速通信）
│   │   ├── bolt_trading.capnp     # 交易相关事件
│   │   ├── bolt_account.capnp     # 账户余额事件  
│   │   ├── bolt_market.capnp      # 市场数据事件
│   │   ├── bolt_system.capnp      # 系统事件
│   │   └── bolt_common.capnp      # 公共类型定义
│   └── proto/                     # Protocol Buffers API 定义（外部接口）
│       ├── bolt_trading_service.proto    # 交易服务API
│       ├── bolt_account_service.proto    # 账户服务API
│       ├── bolt_market_service.proto     # 市场数据API
│       ├── bolt_admin_service.proto      # 管理API
│       └── bolt_common_types.proto       # 公共类型
```

## 🏷️ 命名规范

### Cap'n Proto 命名规范

#### 文件命名：
- 格式：`bolt_{domain}.capnp`
- 示例：`bolt_trading.capnp`, `bolt_account.capnp`

#### OuterClassName：
```capnp
$Java.package("com.cmex.bolt.schema.event");
$Java.outerClassname("Bolt{Domain}Events");
```

#### 事件命名：
- 命令事件：`{Action}{Object}Event` → `PlaceOrderEvent`
- 结果事件：`{Object}{Actioned}Event` → `OrderCreatedEvent`
- 拒绝事件：`{Action}RejectedEvent` → `OrderRejectedEvent`

### Protocol Buffers 命名规范

#### 文件命名：
- 服务定义：`bolt_{domain}_service.proto`
- 类型定义：`bolt_common_types.proto`

#### OuterClassName：
```proto
option java_package = "com.cmex.bolt.schema.grpc.{domain}";
option java_outer_classname = "Bolt{Domain}ServiceProto";
```

#### 消息命名：
- 请求消息：`{Action}{Object}Request` → `PlaceOrderRequest`
- 响应消息：`{Action}{Object}Response` → `PlaceOrderResponse`
- 数据传输：`{Object}Info` → `OrderInfo`, `TradeInfo`

#### 服务命名：
```proto
service Bolt{Domain}Service {
  rpc {ActionObject}({Action}{Object}Request) returns ({Action}{Object}Response);
}
```

## 🎯 领域划分

### 1. Trading Domain（交易领域）
**职责**：订单管理、撮合引擎、成交处理
- **Events**: `PlaceOrderEvent`, `OrderCreatedEvent`, `OrderMatchedEvent`
- **Services**: `PlaceOrder`, `CancelOrder`, `GetOrderHistory`

### 2. Account Domain（账户领域）  
**职责**：余额管理、资金变动、风控
- **Events**: `IncreaseBalanceEvent`, `BalanceIncreasedEvent`, `BalanceFrozenEvent`
- **Services**: `GetBalance`, `IncreaseBalance`, `FreezeBalance`

### 3. Market Domain（市场数据领域）
**职责**：行情数据、深度数据、K线数据
- **Events**: `DepthUpdateEvent`, `TradeTickEvent`, `KlineUpdateEvent`  
- **Services**: `GetDepth`, `GetTicker`, `GetKline`

### 4. System Domain（系统领域）
**职责**：系统监控、配置管理、健康检查
- **Events**: `HeartbeatEvent`, `ConfigUpdateEvent`, `SystemStatusEvent`
- **Services**: `HealthCheck`, `GetSystemInfo`, `UpdateConfig`

## 🔤 编码约定

### 枚举命名
```proto
// Proto3 枚举带前缀，避免冲突
enum OrderType {
  ORDER_TYPE_UNSPECIFIED = 0;
  ORDER_TYPE_LIMIT = 1;
  ORDER_TYPE_MARKET = 2;
}
```

```capnp
# Cap'n Proto 枚举简洁命名
enum OrderType {
  limit @0;
  market @1;
  stopLimit @2;
}
```

### 字段命名
- **Proto**: 使用 `snake_case` → `request_id`, `symbol_id`
- **Cap'n Proto**: 使用 `camelCase` → `requestId`, `symbolId`

### 包结构
```
com.cmex.bolt.schema.
├── event/              # Cap'n Proto 事件类
│   ├── BoltTradingEvents
│   ├── BoltAccountEvents
│   └── BoltMarketEvents
├── grpc.trading/       # gRPC 交易服务
├── grpc.account/       # gRPC 账户服务
├── grpc.market/        # gRPC 市场服务
└── common/             # 公共类型
    └── BoltCommonTypes
```

## ⚡ 性能考虑

### Cap'n Proto（内部事件）
- **零拷贝**：所有事件支持零拷贝序列化
- **紧凑布局**：合理安排字段顺序，减少内存占用
- **联合类型**：使用 union 减少多态开销

### Protocol Buffers（外部API）
- **字段复用**：合理设计可选字段，减少消息大小
- **流式处理**：大量数据查询支持分页和流式响应
- **压缩友好**：字段命名有利于压缩算法

## 🔄 版本管理

### 向后兼容策略
1. **只添加字段，不删除**
2. **新枚举值追加在末尾**
3. **使用 optional/repeated 而非 required**
4. **新功能用新的消息类型**

### 版本号策略
```capnp
# Cap'n Proto 文件ID管理
@0x9eb32e19f86ee174;  # bolt_trading.capnp
@0x8fa23d4e5b9c7610;  # bolt_account.capnp
@0x7dc41f3a8e6b9205;  # bolt_market.capnp
```

## 📝 最佳实践

### 1. 事件设计原则
- **幂等性**：所有事件包含 `requestId` 或 `eventId`
- **可追溯**：包含时间戳和序列号
- **完整性**：包含足够的上下文信息

### 2. API设计原则  
- **RESTful风格**：资源导向的API设计
- **错误处理**：统一的错误码和错误信息
- **安全验证**：请求中包含身份验证信息

### 3. 文档维护
- **及时更新**：Schema变更要同步更新文档
- **示例代码**：提供使用示例和最佳实践
- **迁移指南**：版本升级时提供迁移指南

---

*本规范遵循"约定优于配置"原则，为Bolt交易系统提供清晰、一致、高性能的Schema设计标准。* 