package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SnapshotHandler {

    private final BoltConfig config;
    private final AccountRepository accountRepository;
    private final CurrencyRepository currencyRepository;
    private final SymbolRepository symbolRepository;
    private final ObjectMapper objectMapper;

    public SnapshotHandler(BoltConfig config) {
        this.config = config;
        this.accountRepository = new AccountRepository();
        this.currencyRepository = CurrencyRepository.getInstance();
        this.symbolRepository = SymbolRepository.getInstance();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 处理snapshot事件
     */
    public void handleSnapshot(Nexus.NexusEvent.Reader reader) {
        Nexus.Snapshot.Reader snapshot = reader.getPayload().getSnapshot();
        long timestamp = snapshot.getTimestamp();
        
        log.info("Processing snapshot event with timestamp: {}", timestamp);
        
        try {
            // 创建快照目录
            Path snapshotDir = Paths.get(config.boltHome(), "snapshots");
            Files.createDirectories(snapshotDir);
            
            // 持久化数据
            persistAccountData(timestamp, snapshotDir);
            persistCurrencyData(timestamp, snapshotDir);
            persistSymbolData(timestamp, snapshotDir);
            
            // 更新journal时间戳并生成新journal文件
            updateJournalTimestamp(timestamp);
            
            log.info("Snapshot completed successfully with timestamp: {}", timestamp);
        } catch (Exception e) {
            log.error("Failed to process snapshot with timestamp: {}", timestamp, e);
        }
    }

    /**
     * 持久化账户数据
     */
    private void persistAccountData(long timestamp, Path snapshotDir) throws IOException {
        Map<String, Object> accountData = new HashMap<>();
        accountData.put("timestamp", timestamp);
        accountData.put("accounts", accountRepository.getAllData());
        
        String filename = String.format("accounts_%d.json", timestamp);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, accountData);
        }
        
        log.debug("Account data persisted to: {}", filePath);
    }

    /**
     * 持久化货币数据
     */
    private void persistCurrencyData(long timestamp, Path snapshotDir) throws IOException {
        Map<String, Object> currencyData = new HashMap<>();
        currencyData.put("timestamp", timestamp);
        currencyData.put("currencies", currencyRepository.getAllData());
        
        String filename = String.format("currencies_%d.json", timestamp);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, currencyData);
        }
        
        log.debug("Currency data persisted to: {}", filePath);
    }

    /**
     * 持久化交易对数据
     */
    private void persistSymbolData(long timestamp, Path snapshotDir) throws IOException {
        Map<String, Object> symbolData = new HashMap<>();
        symbolData.put("timestamp", timestamp);
        symbolData.put("symbols", symbolRepository.getAllData());
        
        String filename = String.format("symbols_%d.json", timestamp);
        Path filePath = snapshotDir.resolve(filename);
        
        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, symbolData);
        }
        
        log.debug("Symbol data persisted to: {}", filePath);
    }

    /**
     * 更新journal时间戳并生成新journal文件
     */
    private void updateJournalTimestamp(long timestamp) throws IOException {
        // 在journal文件中记录snapshot时间戳
        Path currentJournal = Paths.get(config.journalFilePath());
        Path journalDir = Paths.get(config.journalDir());
        
        // 生成新的journal文件名
        String newJournalFilename = String.format("journal_%d.data", timestamp);
        Path newJournalFile = journalDir.resolve(newJournalFilename);
        
        // 创建新的journal文件
        Files.createFile(newJournalFile);
        
        // 记录snapshot信息
        String snapshotInfo = String.format("\n--- SNAPSHOT TIMESTAMP: %d ---\n", timestamp);
        Files.write(newJournalFile, snapshotInfo.getBytes());
        
        log.info("Updated journal with snapshot timestamp: {} at {}", timestamp, newJournalFile);
    }
}
