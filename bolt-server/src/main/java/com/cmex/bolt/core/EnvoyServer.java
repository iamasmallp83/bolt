package com.cmex.bolt.core;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.domain.Balance;
import com.cmex.bolt.domain.Currency;
import com.cmex.bolt.domain.Symbol;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.Envoy.*;
import com.cmex.bolt.EnvoyServerGrpc;
import com.cmex.bolt.handler.AccountDispatcher;
import com.cmex.bolt.handler.MatchDispatcher;
import com.cmex.bolt.monitor.RingBufferMonitor;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.service.AccountService;
import com.cmex.bolt.service.MatchService;
import com.cmex.bolt.util.BackpressureManager;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class EnvoyServer extends EnvoyServerGrpc.EnvoyServerImplBase {

    private final RingBuffer<NexusWrapper> accountRingBuffer;
    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    private final List<AccountService> accountServices;
    private final List<MatchService> matchServices;

    // 背压管理器
    private final BackpressureManager accountBackpressureManager;
    private final BackpressureManager matchBackpressureManager;
    private final BackpressureManager responseBackpressureManager;
    private final RingBufferMonitor ringBufferMonitor;
    private final Transfer transfer = new Transfer();

    //分组数量
    private int group = 10;

    public EnvoyServer() {
        observers = new ConcurrentHashMap<>();

        // 创建Disruptor - 优化配置以提升性能
        // 使用2的幂次方大小以优化内存访问模式
        // YieldingWaitStrategy在高吞吐量场景下比BusySpinWaitStrategy更节省CPU
        Disruptor<NexusWrapper> accountDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), 1024 * 512, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new BusySpinWaitStrategy());
        Disruptor<NexusWrapper> matchDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), 1024 * 256, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new BusySpinWaitStrategy());
        Disruptor<NexusWrapper> responseDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), 1024 * 256, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new BusySpinWaitStrategy()); // Response通常是单生产者

        List<AccountDispatcher> accountDispatchers = createAccountDispatchers();
        List<MatchDispatcher> matchDispatchers = createMatchDispatchers();
        matchDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        accountDisruptor.handleEventsWith(accountDispatchers.toArray(new AccountDispatcher[0]));

        accountRingBuffer = accountDisruptor.start();
        RingBuffer<NexusWrapper> matchRingBuffer = matchDisruptor.start();
        RingBuffer<NexusWrapper> responseRingBuffer = responseDisruptor.start();

        // 初始化背压管理器
        accountBackpressureManager = new BackpressureManager("Account", accountRingBuffer);
        matchBackpressureManager = new BackpressureManager("Match", matchRingBuffer);
        responseBackpressureManager = new BackpressureManager("Response", responseRingBuffer);

        // 初始化监控器
        ringBufferMonitor = new RingBufferMonitor(5000); // 5秒报告一次
        ringBufferMonitor.addManager(accountBackpressureManager);
        ringBufferMonitor.addManager(matchBackpressureManager);
        ringBufferMonitor.addManager(responseBackpressureManager);
        ringBufferMonitor.startMonitoring();

        matchServices = new ArrayList<>(group);
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getMatchService().setSequencerRingBuffer(accountRingBuffer);
            dispatcher.getMatchService().setResponseRingBuffer(responseRingBuffer);
            matchServices.add(dispatcher.getMatchService());
        }
        accountServices = new ArrayList<>(group);
        for (AccountDispatcher dispatcher : accountDispatchers) {
            dispatcher.getAccountService().setMatchRingBuffer(matchRingBuffer);
            dispatcher.getAccountService().setResponseRingBuffer(responseRingBuffer);
            dispatcher.setMatchServices(matchServices);
            accountServices.add(dispatcher.getAccountService());
        }
    }

    private List<AccountDispatcher> createAccountDispatchers() {
        List<AccountDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < group; i++) {
            dispatchers.add(new AccountDispatcher(group, i));
        }
        return dispatchers;
    }

    private List<MatchDispatcher> createMatchDispatchers() {
        List<MatchDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < group; i++) {
            dispatchers.add(new MatchDispatcher(group, i));
        }
        return dispatchers;
    }

    private AccountService getAccountService(int accountId) {
        return accountServices.get(getPartition(accountId));
    }

    private int getPartition(int accountId) {
        return accountId % group;
    }

    /**
     * 处理系统繁忙的响应
     */
    private void handleSystemBusy(StreamObserver<?> responseObserver, String operationType) {
        switch (operationType) {
            case "PLACE_ORDER" -> {
                PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.SYSTEM_BUSY.ordinal())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<PlaceOrderResponse> observer = (StreamObserver<PlaceOrderResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "INCREASE" -> {
                IncreaseResponse response = IncreaseResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.SYSTEM_BUSY.ordinal())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<IncreaseResponse> observer = (StreamObserver<IncreaseResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "DECREASE" -> {
                DecreaseResponse response = DecreaseResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.SYSTEM_BUSY.ordinal())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<DecreaseResponse> observer = (StreamObserver<DecreaseResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "CANCEL_ORDER" -> {
                CancelOrderResponse response = CancelOrderResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.SYSTEM_BUSY.ordinal())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<CancelOrderResponse> observer = (StreamObserver<CancelOrderResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
        }
    }

    @Override
    public void getAccount(GetAccountRequest request, StreamObserver<GetAccountResponse> responseObserver) {
        Map<Integer, Envoy.Balance> balances =
                getAccountService(request.getAccountId()).getBalances(request.getAccountId(), request.getCurrencyId()).entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    Balance balance = entry.getValue();
                                    return Envoy.Balance.newBuilder()
                                            .setCurrency(balance.getCurrency().getName())
                                            .setFrozen(balance.getFormatFrozen())
                                            .setAvailable(balance.getFormatAvailable())
                                            .setValue(balance.getFormatValue())
                                            .build();
                                }));
        GetAccountResponse response = GetAccountResponse.newBuilder()
                .setCode(1)
                .putAllData(balances)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void increase(IncreaseRequest request, StreamObserver<IncreaseResponse> responseObserver) {
        // 检查背压状态
        if (!accountBackpressureManager.canAcceptRequest()) {
            handleSystemBusy(responseObserver, "INCREASE");
            return;
        }

        getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId()).ifPresentOrElse(currency -> {
            long id = requestId.incrementAndGet();
            int partition = getPartition(request.getAccountId());
            observers.put(id, responseObserver);
            accountRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                wrapper.setPartition(partition);
                transfer.write(request, currency, wrapper.getBuffer());
            });
        }, () -> {
            IncreaseResponse response = IncreaseResponse.newBuilder()
                    .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                    .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    public void decrease(DecreaseRequest request, StreamObserver<DecreaseResponse> responseObserver) {
        // 检查背压状态
        if (!accountBackpressureManager.canAcceptRequest()) {
            handleSystemBusy(responseObserver, "DECREASE");
            return;
        }

        getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId()).ifPresentOrElse(currency -> {
            long id = requestId.incrementAndGet();
            observers.put(id, responseObserver);
            accountRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                transfer.write(request, currency, wrapper.getBuffer());
            });
        }, () -> {
            DecreaseResponse response = DecreaseResponse.newBuilder()
                    .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                    .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        // 检查背压状态
        BackpressureManager.BackpressureResult result = accountBackpressureManager.checkCapacity();
        if (result == BackpressureManager.BackpressureResult.CRITICAL_REJECT) {
            handleSystemBusy(responseObserver, "PLACE_ORDER");
            return;
        }

        // 如果是高负载，输出警告日志
        if (result == BackpressureManager.BackpressureResult.HIGH_LOAD) {
            System.out.printf("WARNING: Account RingBuffer usage is high: %.2f%%%n",
                    accountBackpressureManager.getCurrentUsageRate() * 100);
        }

        getSymbol(request.getSymbolId()).ifPresentOrElse(symbol -> {
            long id = requestId.incrementAndGet();
            observers.put(id, responseObserver);
            accountRingBuffer.publishEvent((message, sequence) -> {
            });
        }, () -> {
            PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                    .setCode(Nexus.RejectionReason.SYMBOL_NOT_EXIST.ordinal())
                    .setMessage(Nexus.RejectionReason.SYMBOL_NOT_EXIST.name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> responseObserver) {
        // 检查背压状态
        if (!accountBackpressureManager.canAcceptRequest()) {
            handleSystemBusy(responseObserver, "CANCEL_ORDER");
            return;
        }

        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
        });
    }

    @Override
    public void getDepth(Envoy.GetDepthRequest request, StreamObserver<Envoy.GetDepthResponse> responseObserver) {
        int symbolId = request.getSymbolId();
        MatchService matchService = matchServices.get(symbolId % group);
        DepthDto dto = matchService.getDepth(symbolId);
        GetDepthResponse response = GetDepthResponse.newBuilder()
                .setCode(1)
                .setData(Depth.newBuilder().setSymbol(dto.getSymbol()).putAllAsks(dto.getAsks()).putAllBids(dto.getBids()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 获取背压统计信息 - 用于外部监控
     */
    public BackpressureManager.BackpressureStats getAccountBackpressureStats() {
        return accountBackpressureManager.getStats();
    }

    public BackpressureManager.BackpressureStats getMatchBackpressureStats() {
        return matchBackpressureManager.getStats();
    }

    public BackpressureManager.BackpressureStats getResponseBackpressureStats() {
        return responseBackpressureManager.getStats();
    }

    /**
     * 获取监控器的汇总统计
     */
    public RingBufferMonitor.SummaryStats getMonitorSummary() {
        return ringBufferMonitor.getSummaryStats();
    }

    /**
     * 立即输出监控报告
     */
    public void reportMonitorStats() {
        ringBufferMonitor.reportStats();
    }

    /**
     * 重置所有统计信息
     */
    public void resetAllStats() {
        accountBackpressureManager.resetStats();
        matchBackpressureManager.resetStats();
        responseBackpressureManager.resetStats();
    }

    /**
     * 关闭监控器（在服务停止时调用）
     */
    public void shutdown() {
        ringBufferMonitor.stopMonitoring();
    }

    private class ResponseEventHandler implements EventHandler<NexusWrapper>, LifecycleAware {

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
            long id = wrapper.getId();
            if (id > 0) {
                StreamObserver<Object> observer = (StreamObserver<Object>) observers.get(id);
                Object object = transfer.to(CurrencyRepository.getInstance(), wrapper.getBuffer());
                observer.onNext(object);
                observer.onCompleted();
            }
        }

        @Override
        public void onStart() {
            final Thread currentThread = Thread.currentThread();
            currentThread.setName(ResponseEventHandler.class.getSimpleName() + "-thread");
        }

        @Override
        public void onShutdown() {

        }
    }

    private Optional<Symbol> getSymbol(int symbolId) {
        return matchServices.get(symbolId % 10).getSymbol(symbolId);
    }
}
