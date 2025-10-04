package com.cmex.bolt.recovery;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.domain.Account;
import com.cmex.bolt.domain.Balance;
import com.cmex.bolt.domain.Currency;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;

@Slf4j
public class DataRecovery {

    private final BoltConfig config;
    private final SnapshotReader snapshotReader;
    private final ObjectMapper objectMapper;

    // Repository实例
    private final AccountRepository accountRepository;
    private final CurrencyRepository currencyRepository;
    private final SymbolRepository symbolRepository;

    public DataRecovery(BoltConfig config) {
        this.config = config;
        this.snapshotReader = new SnapshotReader(config);
        this.objectMapper = new ObjectMapper();
        this.accountRepository = new AccountRepository();
        this.currencyRepository = CurrencyRepository.getInstance();
        this.symbolRepository = SymbolRepository.getInstance();
    }

    /**
     * 执行完整的数据恢复
     */
    public long recoverFromSnapshot() throws IOException {
        log.info("Starting data recovery from snapshot...");
        
        // 查找最新的snapshot
        SnapshotReader.SnapshotInfo snapshotInfo = snapshotReader.findLatestSnapshot();
        
        if (snapshotInfo == null) {
            log.info("No snapshot found, starting with fresh data");
            return -1; // 没有snapshot，返回-1表示从头开始
        }

        log.info("Found snapshot with timestamp: {}, recovering data...", snapshotInfo.getTimestamp());

        // 按顺序恢复数据
        recoverCurrencies(snapshotInfo.getCurrencyFile());
        recoverSymbols(snapshotInfo.getSymbolFile());
        recoverAccounts(snapshotInfo.getAccountFile());
        
        log.info("Data recovery completed successfully from snapshot: {}", snapshotInfo.getTimestamp());
        return snapshotInfo.getTimestamp();
    }

    /**
     * 恢复货币数据
     */
    private void recoverCurrencies(Path currencyFile) throws IOException {
        Map<String, Object> currencyData = snapshotReader.readCurrencyData(currencyFile);
        
        if (currencyData == null || currencyData.isEmpty()) {
            log.info("No currency data in snapshot, using defaults");
            return;
        }

        log.debug("Restoring {} currencies from snapshot", currencyData.size());
        
        // 注意：CurrencyRepository是单例，我们需要重新初始化
        // 这里我们清空现有数据并重新加载
        currencyRepository.getAllData().clear();
        
        for (Map.Entry<String, Object> entry : currencyData.entrySet()) {
            try {
                JsonNode currencyNode = objectMapper.valueToTree(entry.getValue());
                
                Currency currency = Currency.builder()
                    .id(currencyNode.get("id").asInt())
                    .name(currencyNode.get("name").asText())
                    .precision(currencyNode.get("precision").asInt())
                    .build();
                
                currencyRepository.getOrCreate(currency.getId(), currency);
                
            } catch (Exception e) {
                log.error("Failed to restore currency: {}", entry.getKey(), e);
            }
        }
        
        log.info("Successfully restored {} currencies", currencyData.size());
    }

    /**
     * 恢复交易对数据
     */
    private void recoverSymbols(Path symbolFile) throws IOException {
        Map<String, Object> symbolData = snapshotReader.readSymbolData(symbolFile);
        
        if (symbolData == null || symbolData.isEmpty()) {
            log.info("No symbol data in snapshot, using defaults");
            return;
        }

        log.debug("Restoring {} symbols from snapshot", symbolData.size());
        
        // 清空现有symbol数据
        symbolRepository.getAllData().clear();
        
        for (Map.Entry<String, Object> entry : symbolData.entrySet()) {
            try {
                JsonNode symbolNode = objectMapper.valueToTree(entry.getValue());
                
                // 获取base和quote currency
                JsonNode baseNode = symbolNode.get("base");
                JsonNode quoteNode = symbolNode.get("quote");
                
                Currency base = currencyRepository.get(baseNode.get("id").asInt()).orElse(null);
                Currency quote = currencyRepository.get(quoteNode.get("id").asInt()).orElse(null);
                
                if (base == null || quote == null) {
                    log.error("Missing base or quote currency for symbol: {}", entry.getKey());
                    continue;
                }
                
                // 创建Symbol
                com.cmex.bolt.domain.Symbol symbol = com.cmex.bolt.domain.Symbol.builder()
                    .id(symbolNode.get("id").asInt())
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
                
                symbolRepository.getOrCreate(symbol.getId(), symbol);
                
            } catch (Exception e) {
                log.error("Failed to restore symbol: {}", entry.getKey(), e);
            }
        }
        
        log.info("Successfully restored {} symbols", symbolData.size());
    }

    /**
     * 恢复账户数据
     */
    private void recoverAccounts(Path accountFile) throws IOException {
        Map<String, Object> accountData = snapshotReader.readAccountData(accountFile);
        
        if (accountData == null || accountData.isEmpty()) {
            log.info("No account data in snapshot, using defaults");
            return;
        }

        log.debug("Restoring {} accounts from snapshot", accountData.size());
        
        for (Map.Entry<String, Object> entry : accountData.entrySet()) {
            try {
                JsonNode accountNode = objectMapper.valueToTree(entry.getValue());
                int accountId = accountNode.get("id").asInt();
                
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
                log.error("Failed to restore account: {}", entry.getKey(), e);
            }
        }
        
        log.info("Successfully restored {} accounts", accountData.size());
    }

    /**
     * 恢复OrderBook状态
     */
    private void restoreOrderBook(com.cmex.bolt.domain.Symbol symbol, JsonNode orderBookNode) {
        try {
            // 这里可以根据需要恢复具体的订单簿状态
            // 例如：恢复挂单、价格区间等
            log.debug("Restoring OrderBook for symbol: {}", symbol.getName());
            
            // TODO: 实现具体的OrderBook状态恢复逻辑
            // 这可能需要恢复bid/ask队列、价格区间等信息
            
        } catch (Exception e) {
            log.error("Failed to restore OrderBook for symbol: {}", symbol.getName(), e);
        }
    }

    /**
     * 获取恢复后的repository实例
     */
    public AccountRepository getAccountRepository() {
        return accountRepository;
    }

    public CurrencyRepository getCurrencyRepository() {
        return currencyRepository;
    }

    public SymbolRepository getSymbolRepository() {
        return symbolRepository;
    }
}
