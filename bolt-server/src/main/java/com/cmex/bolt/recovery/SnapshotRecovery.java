package com.cmex.bolt.recovery;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class SnapshotRecovery {

    private final BoltConfig config;
    private final SnapshotReader snapshotReader;
    private final ObjectMapper objectMapper;

    public SnapshotRecovery(BoltConfig config) {
        this.config = config;
        this.snapshotReader = new SnapshotReader(config);
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 执行完整的数据恢复
     */
    public SnapshotData recoverFromSnapshot() throws IOException {
        log.info("Starting data recovery from snapshot...");

        // 查找最新的snapshot
        SnapshotReader.SnapshotInfo snapshotInfo = snapshotReader.findLatestSnapshot();

        if (snapshotInfo == null) {
            log.info("No snapshot found, starting with fresh data");
            return createEmptySnapshotData();
        }

        log.info("Found snapshot with timestamp: {}, recovering data...", snapshotInfo.getTimestamp());

        // 为每个 partition 创建 repository
        List<AccountRepository> accountRepositories = new ArrayList<>();
        List<CurrencyRepository> currencyRepositories = new ArrayList<>();
        List<SymbolRepository> symbolRepositories = new ArrayList<>();

        for (int i = 0; i < config.group(); i++) {
            // 每个 partition 都有自己的 repository 实例
            CurrencyRepository currencyRepository = recoverCurrenciesForPartition(snapshotInfo.getCurrencyFile(i), i);
            SymbolRepository symbolRepository = recoverSymbolsForPartition(snapshotInfo.getSymbolFile(i), currencyRepository, i);
            AccountRepository accountRepository = recoverAccountsForPartition(snapshotInfo.getAccountFile(i), currencyRepository, i);

            accountRepositories.add(accountRepository);
            currencyRepositories.add(currencyRepository);
            symbolRepositories.add(symbolRepository);
        }

        log.info("Data recovery completed successfully from snapshot: {}", snapshotInfo.getTimestamp());
        return new SnapshotData(snapshotInfo.getTimestamp(), accountRepositories, currencyRepositories, symbolRepositories);
    }

    /**
     * 创建空的SnapshotData
     */
    private SnapshotData createEmptySnapshotData() {
        List<AccountRepository> accountRepositories = new ArrayList<>();
        List<CurrencyRepository> currencyRepositories = new ArrayList<>();
        List<SymbolRepository> symbolRepositories = new ArrayList<>();

        for (int i = 0; i < config.group(); i++) {
            accountRepositories.add(new AccountRepository());
            CurrencyRepository currencyRepository = new CurrencyRepository();
            currencyRepositories.add(currencyRepository);
            symbolRepositories.add(new SymbolRepository(currencyRepository));
        }

        return new SnapshotData(-1, accountRepositories, currencyRepositories, symbolRepositories);
    }

    /**
     * 恢复指定 partition 的货币数据
     */
    private CurrencyRepository recoverCurrenciesForPartition(Path currencyFile, int partition) throws IOException {
        Map<String, Object> currencyData = snapshotReader.readCurrencyData(currencyFile);

        CurrencyRepository currencyRepository = new CurrencyRepository();

        if (currencyData == null || currencyData.isEmpty()) {
            log.info("No currency data in snapshot for partition {}, using defaults", partition);
            return currencyRepository;
        }

        log.debug("Restoring currencies for partition {} from snapshot", partition);

        for (Map.Entry<String, Object> entry : currencyData.entrySet()) {
            try {
                JsonNode currencyNode = objectMapper.valueToTree(entry.getValue());
                int currencyId = currencyNode.get("id").asInt();

                // 只恢复属于当前 partition 的货币
                if (currencyId % config.group() != partition) {
                    continue;
                }

                Currency currency = Currency.builder()
                        .id(currencyId)
                        .name(currencyNode.get("name").asText())
                        .precision(currencyNode.get("precision").asInt())
                        .build();

                currencyRepository.getOrCreate(currency.getId(), currency);

            } catch (Exception e) {
                log.error("Failed to restore currency: {} for partition {}", entry.getKey(), partition, e);
            }
        }

        log.info("Successfully restored currencies for partition {}", partition);
        return currencyRepository;
    }

    /**
     * 恢复指定 partition 的账户数据
     */
    private AccountRepository recoverAccountsForPartition(Path accountFile, CurrencyRepository currencyRepository, int partition) throws IOException {
        Map<String, Object> accountData = snapshotReader.readAccountData(accountFile);

        AccountRepository accountRepository = new AccountRepository();

        if (accountData == null || accountData.isEmpty()) {
            log.info("No account data in snapshot for partition {}", partition);
            return accountRepository;
        }

        log.debug("Restoring accounts for partition {} from snapshot", partition);

        for (Map.Entry<String, Object> entry : accountData.entrySet()) {
            try {
                JsonNode accountNode = objectMapper.valueToTree(entry.getValue());
                int accountId = accountNode.get("id").asInt();

                // 只恢复属于当前 partition 的账户
                if (accountId % config.group() != partition) {
                    continue;
                }

                Account account = new Account(accountId);

                // 恢复余额数据
                JsonNode balancesNode = accountNode.get("balances");
                if (balancesNode != null) {
                    for (JsonNode balanceNode : balancesNode) {
                        JsonNode currencyNode = balanceNode.get("currency");
                        Currency currency = currencyRepository.get(currencyNode.get("id").asInt()).orElse(null);

                        if (currency != null) {
                            BigDecimal value = new BigDecimal(balanceNode.get("value").asText());
                            BigDecimal frozen = new BigDecimal(balanceNode.get("frozen").asText());

                            Balance balance = new Balance(currency, value, frozen);
                            account.addBalance(balance);
                        }
                    }
                }

                accountRepository.getOrCreate(accountId, account);

            } catch (Exception e) {
                log.error("Failed to restore account: {} for partition {}", entry.getKey(), partition, e);
            }
        }

        log.info("Successfully restored accounts for partition {}", partition);
        return accountRepository;
    }

    /**
     * 恢复指定 partition 的交易对数据
     */
    private SymbolRepository recoverSymbolsForPartition(Path symbolFile, CurrencyRepository currencyRepository, int partition) throws IOException {
        Map<String, Object> symbolData = snapshotReader.readSymbolData(symbolFile);

        SymbolRepository symbolRepository = new SymbolRepository(currencyRepository);

        if (symbolData == null || symbolData.isEmpty()) {
            log.info("No symbol data in snapshot for partition {}", partition);
            return symbolRepository;
        }

        log.debug("Restoring symbols for partition {} from snapshot", partition);

        for (Map.Entry<String, Object> entry : symbolData.entrySet()) {
            try {
                JsonNode symbolNode = objectMapper.valueToTree(entry.getValue());
                int symbolId = symbolNode.get("id").asInt();

                // 只恢复属于当前 partition 的交易对
                if (symbolId % config.group() != partition) {
                    continue;
                }

                // 获取base和quote currency
                JsonNode baseNode = symbolNode.get("base");
                JsonNode quoteNode = symbolNode.get("quote");

                Currency base = currencyRepository.get(baseNode.get("id").asInt()).orElse(null);
                Currency quote = currencyRepository.get(quoteNode.get("id").asInt()).orElse(null);

                if (base == null || quote == null) {
                    log.error("Missing base or quote currency for symbol: {} in partition {}", entry.getKey(), partition);
                    continue;
                }

                // 创建Symbol
                Symbol symbol = Symbol.builder()
                        .id(symbolId)
                        .name(symbolNode.get("name").asText())
                        .base(base)
                        .quote(quote)
                        .quoteSettlement(symbolNode.get("quoteSettlement").asBoolean())
                        .build();

                symbol.init(); // 初始化OrderBook

                // 恢复OrderBook状态
                JsonNode orderBookNode = symbolNode.get("orderBook");
                if (orderBookNode != null) {
                    restoreOrderBook(symbol, orderBookNode);
                }
                //TODO 临时办法
                symbolRepository.remove(symbol.getId());
                symbolRepository.getOrCreate(symbol.getId(), symbol);

            } catch (Exception e) {
                log.error("Failed to restore symbol: {} for partition {}", entry.getKey(), partition, e);
            }
        }

        log.info("Successfully restored symbols for partition {}", partition);
        return symbolRepository;
    }

    /**
     * 恢复OrderBook状态
     * 注意：由于Order类是不可变的，我们只能恢复基本的订单信息
     * 动态状态（如已成交量、可用量等）将在系统重新启动后通过重新处理日志来恢复
     */
    private void restoreOrderBook(Symbol symbol, JsonNode orderBookNode) {
        try {
            log.debug("Restoring OrderBook for symbol: {}", symbol.getName());
            
            OrderBook orderBook = symbol.getOrderBook();
            
            // 恢复bids（买单）
            JsonNode bidsNode = orderBookNode.get("bids");
            if (bidsNode != null) {
                restorePriceLevels(orderBook, bidsNode, Order.Side.BID);
            }
            
            // 恢复asks（卖单）
            JsonNode asksNode = orderBookNode.get("asks");
            if (asksNode != null) {
                restorePriceLevels(orderBook, asksNode, Order.Side.ASK);
            }
            
            // 恢复订单映射
            JsonNode ordersNode = orderBookNode.get("orders");
            if (ordersNode != null) {
                restoreOrdersMapping(orderBook, ordersNode);
            }
            
            log.debug("Successfully restored OrderBook for symbol: {} with {} bids and {} asks",
                     symbol.getName(), 
                     orderBook.getBids().size(), 
                     orderBook.getAsks().size());

        } catch (Exception e) {
            log.error("Failed to restore OrderBook for symbol: {}", symbol.getName(), e);
        }
    }
    
    /**
     * 恢复价格层级（bids或asks）
     */
    private void restorePriceLevels(OrderBook orderBook, JsonNode priceLevelsNode, Order.Side side) {
        Iterator<Map.Entry<String, JsonNode>> fields = priceLevelsNode.fields();
        
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            try {
                BigDecimal price = new BigDecimal(entry.getKey());
                JsonNode priceNodeData = entry.getValue();
                
                // 创建PriceNode
                PriceNode priceNode = null;
                
                // 恢复该价格层级的所有订单
                JsonNode ordersNode = priceNodeData.get("orders");
                if (ordersNode != null && ordersNode.isArray()) {
                    for (JsonNode orderNode : ordersNode) {
                        Order order = restoreOrderFromJson(orderNode);
                        if (order != null) {
                            if (priceNode == null) {
                                priceNode = new PriceNode(price, order);
                            } else {
                                priceNode.add(order);
                            }
                            
                            // 将订单添加到订单簿的订单映射中
                            orderBook.getOrders().put(order.getId(), order);
                        }
                    }
                }
                
                if (priceNode != null) {
                    // 将PriceNode添加到对应的TreeMap中
                    if (side == Order.Side.BID) {
                        orderBook.getBids().put(price, priceNode);
                    } else {
                        orderBook.getAsks().put(price, priceNode);
                    }
                }
                
            } catch (Exception e) {
                log.error("Failed to restore price level: {} for side: {}", entry.getKey(), side, e);
            }
        }
    }
    
    /**
     * 恢复订单映射（用于快速查找订单）
     */
    private void restoreOrdersMapping(OrderBook orderBook, JsonNode ordersNode) {
        Iterator<Map.Entry<String, JsonNode>> fields = ordersNode.fields();
        
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            try {
                long orderId = Long.parseLong(entry.getKey());
                JsonNode orderNode = entry.getValue();
                
                Order order = restoreOrderFromJson(orderNode);
                if (order != null) {
                    orderBook.getOrders().put(orderId, order);
                }
                
            } catch (Exception e) {
                log.error("Failed to restore order mapping for orderId: {}", entry.getKey(), e);
            }
        }
    }
    
    /**
     * 从JSON恢复Order对象
     * 恢复订单的基本信息和动态状态属性
     */
    private Order restoreOrderFromJson(JsonNode orderNode) {
        try {
            long id = orderNode.get("id").asLong();
            int symbolId = orderNode.get("symbolId").asInt();
            int accountId = orderNode.get("accountId").asInt();
            
            // 恢复订单类型
            String typeStr = orderNode.get("type").asText();
            Order.Type type = Order.Type.valueOf(typeStr);
            
            // 恢复订单方向
            String sideStr = orderNode.get("side").asText();
            Order.Side side = Order.Side.valueOf(sideStr);
            
            // 恢复订单规格
            JsonNode specNode = orderNode.get("specification");
            Order.Specification specification = restoreSpecificationFromJson(specNode);
            
            // 恢复费率
            JsonNode feeNode = orderNode.get("fee");
            Order.Fee fee = restoreFeeFromJson(feeNode);
            
            // 恢复冻结金额
            BigDecimal frozen = new BigDecimal(orderNode.get("frozen").asText());
            
            // 创建订单（恢复基本信息）
            Order order = Order.builder()
                    .id(id)
                    .symbolId(symbolId)
                    .accountId(accountId)
                    .type(type)
                    .side(side)
                    .specification(specification)
                    .fee(fee)
                    .frozen(frozen)
                    .build();
            
            // 恢复动态状态属性
            if (orderNode.has("status")) {
                String statusStr = orderNode.get("status").asText();
                Order.OrderStatus status = Order.OrderStatus.valueOf(statusStr);
                order.setStatus(status);
            }
            
            if (orderNode.has("availableQuantity")) {
                BigDecimal availableQuantity = new BigDecimal(orderNode.get("availableQuantity").asText());
                order.setAvailableQuantity(availableQuantity);
            }
            
            if (orderNode.has("availableAmount")) {
                BigDecimal availableAmount = new BigDecimal(orderNode.get("availableAmount").asText());
                order.setAvailableAmount(availableAmount);
            }
            
            if (orderNode.has("cost")) {
                BigDecimal cost = new BigDecimal(orderNode.get("cost").asText());
                order.setCost(cost);
            }
            
            if (orderNode.has("executedQuantity")) {
                BigDecimal executedQuantity = new BigDecimal(orderNode.get("executedQuantity").asText());
                order.setExecutedQuantity(executedQuantity);
            }
            
            if (orderNode.has("executedVolume")) {
                BigDecimal executedVolume = new BigDecimal(orderNode.get("executedVolume").asText());
                order.setExecutedVolume(executedVolume);
            }
            
            return order;
            
        } catch (Exception e) {
            log.error("Failed to restore order from JSON", e);
            return null;
        }
    }
    
    /**
     * 从JSON恢复订单规格
     */
    private Order.Specification restoreSpecificationFromJson(JsonNode specNode) {
        BigDecimal price = new BigDecimal(specNode.get("price").asText());
        BigDecimal quantity = new BigDecimal(specNode.get("quantity").asText());
        BigDecimal amount = new BigDecimal(specNode.get("amount").asText());
        
        String quantityTypeStr = specNode.get("quantityType").asText();
        Order.Specification.QuantityType quantityType = 
                Order.Specification.QuantityType.valueOf(quantityTypeStr);
        
        // 使用静态工厂方法创建Specification
        if (quantityType == Order.Specification.QuantityType.BY_QUANTITY) {
            if (price.compareTo(BigDecimal.ZERO) > 0) {
                // LIMIT订单
                return Order.Specification.limitByQuantity(price, quantity);
            } else {
                // MARKET订单按数量
                return Order.Specification.marketByQuantity(quantity);
            }
        } else {
            // MARKET订单按金额
            return Order.Specification.marketByAmount(amount);
        }
    }
    
    /**
     * 从JSON恢复费率
     */
    private Order.Fee restoreFeeFromJson(JsonNode feeNode) {
        int takerRate = feeNode.get("taker").asInt();
        int makerRate = feeNode.get("maker").asInt();
        
        return Order.Fee.builder()
                .taker(takerRate)
                .maker(makerRate)
                .build();
    }
    
}
