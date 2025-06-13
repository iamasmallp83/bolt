package com.cmex.bolt.util;

/**
 * 通用的背压检查方法
 */
@FunctionalInterface
public interface SystemBusyResponseFactory<T> {
    T createResponse();
}