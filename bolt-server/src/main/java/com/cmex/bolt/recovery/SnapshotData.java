package com.cmex.bolt.recovery;

import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;

import java.util.List;

/**
 * Snapshot数据封装类
 * 包含从snapshot恢复的所有repository数据
 */
public record SnapshotData(
        long timestamp,
        List<AccountRepository> accountRepositories,
        List<CurrencyRepository> currencyRepositories,
        List<SymbolRepository> symbolRepositories
) {
    
    /**
     * 创建空的SnapshotData（没有snapshot时使用）
     */
    public static SnapshotData empty() {
        return new SnapshotData(-1, List.of(), List.of(), List.of());
    }
    
    /**
     * 检查是否有有效的snapshot数据
     */
    public boolean hasValidSnapshot() {
        return timestamp > 0 && !accountRepositories.isEmpty();
    }
}