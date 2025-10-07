package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.service.AccountService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SequencerSnapshot {

    private final BoltConfig config;
    private final AccountService accountService;
    private final ObjectMapper objectMapper;

    public SequencerSnapshot(BoltConfig config, AccountService accountService) {
        this.config = config;
        this.accountService = accountService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 处理snapshot事件
     */
    public void handleSnapshot(Nexus.NexusEvent.Reader reader, int partition) {
        Nexus.Snapshot.Reader snapshot = reader.getPayload().getSnapshot();
        long timestamp = snapshot.getTimestamp();
        
        log.info("Processing sequencer snapshot event with timestamp: {}, partition: {}", timestamp, partition);
        
        try {
            // 创建快照目录结构：sequencer_snapshots/{timestamp}/
            Path baseSnapshotDir = Paths.get(config.boltHome(), "sequencer_snapshots");
            Path timestampSnapshotDir = baseSnapshotDir.resolve(String.valueOf(timestamp));
            Files.createDirectories(timestampSnapshotDir);
            
            // 持久化数据
            persistAccountData(timestamp, partition, timestampSnapshotDir);
            persistCurrencyData(timestamp, partition, timestampSnapshotDir);
            persistSymbolData(timestamp, partition, timestampSnapshotDir);
            
            log.info("Sequencer snapshot completed successfully with timestamp: {}, partition: {}", timestamp, partition);
        } catch (Exception e) {
            log.error("Failed to process sequencer snapshot with timestamp: {}, partition: {}", timestamp, partition, e);
        }
    }

    /**
     * 持久化账户数据
     */
    private void persistAccountData(long timestamp, int partition, Path snapshotDir) throws IOException {
        Map<String, Object> accountData = new HashMap<>();
        accountData.put("timestamp", timestamp);
        accountData.put("partition", partition);
        accountData.put("accounts", accountService.getAccountRepository().getAllData());
        
        String filename = String.format("account_%d", partition);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, accountData);
        }
        
        log.debug("Account data persisted to: {}", filePath);
    }

    /**
     * 持久化货币数据
     */
    private void persistCurrencyData(long timestamp, int partition, Path snapshotDir) throws IOException {
        Map<String, Object> currencyData = new HashMap<>();
        currencyData.put("timestamp", timestamp);
        currencyData.put("partition", partition);
        currencyData.put("currencies", accountService.getCurrencyRepository().getAllData());
        
        String filename = String.format("currency_%d", partition);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, currencyData);
        }
        
        log.debug("Currency data persisted to: {}", filePath);
    }

    /**
     * 持久化交易对数据
     */
    private void persistSymbolData(long timestamp, int partition, Path snapshotDir) throws IOException {
        Map<String, Object> symbolData = new HashMap<>();
        symbolData.put("timestamp", timestamp);
        symbolData.put("partition", partition);
        symbolData.put("symbols", accountService.getSymbolRepository().getAllData());
        
        String filename = String.format("symbol_%d", partition);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, symbolData);
        }
        
        log.debug("Symbol data persisted to: {}", filePath);
    }
}