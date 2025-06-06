package com.cmex.bolt.spot.grpc;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.handler.*;
import com.cmex.bolt.spot.domain.Balance;
import com.cmex.bolt.spot.domain.Symbol;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.service.AccountService;
import com.cmex.bolt.spot.service.MatchService;
import com.cmex.bolt.spot.util.BackpressureManager;
import com.cmex.bolt.spot.util.MemoryLeakDetector;
import com.cmex.bolt.spot.monitor.RingBufferMonitor;
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

import static com.cmex.bolt.spot.grpc.SpotServiceGrpc.SpotServiceImplBase;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.*;

public class SpotServiceImpl extends SpotServiceImplBase {

    private final RingBuffer<Message> accountRingBuffer;
    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    private final List<AccountService> accountServices;
    private final List<MatchService> matchServices;
    
    // 背压管理器
    private final BackpressureManager accountBackpressureManager;
    private final BackpressureManager matchBackpressureManager;
    private final BackpressureManager responseBackpressureManager;
    private final RingBufferMonitor ringBufferMonitor;
    private final MemoryLeakDetector memoryLeakDetector;

    public SpotServiceImpl() {
        observers = new ConcurrentHashMap<>();
        
        // 创建Disruptor - 优化配置以提升性能
        // 使用2的幂次方大小以优化内存访问模式
        // YieldingWaitStrategy在高吞吐量场景下比BusySpinWaitStrategy更节省CPU
        Disruptor<Message> accountDisruptor =
                new Disruptor<>(Message.FACTORY, 1024 * 512, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new YieldingWaitStrategy());
        Disruptor<Message> matchDisruptor =
                new Disruptor<>(Message.FACTORY, 1024 * 256, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new YieldingWaitStrategy());
        Disruptor<Message> responseDisruptor =
                new Disruptor<>(Message.FACTORY, 1024 * 256, DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, new YieldingWaitStrategy()); // Response通常是单生产者
        
        List<AccountDispatcher> accountDispatchers = createAccountDispatchers();
        List<MatchDispatcher> matchDispatchers = createMatchDispatchers();
        matchDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        accountDisruptor.handleEventsWith(accountDispatchers.toArray(new AccountDispatcher[0]));
        
        accountRingBuffer = accountDisruptor.start();
        RingBuffer<Message> matchRingBuffer = matchDisruptor.start();
        RingBuffer<Message> responseRingBuffer = responseDisruptor.start();
        
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
        
        // 初始化并启动内存泄漏检测器
        memoryLeakDetector = new MemoryLeakDetector();
        memoryLeakDetector.setObserversToMonitor(observers);
        memoryLeakDetector.startMonitoring();
        
        matchServices = new ArrayList<>(10);
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getMatchService().setSequencerRingBuffer(accountRingBuffer);
            dispatcher.getMatchService().setResponseRingBuffer(responseRingBuffer);
            matchServices.add(dispatcher.getMatchService());
        }
        accountServices = new ArrayList<>(10);
        for (AccountDispatcher dispatcher : accountDispatchers) {
            dispatcher.getAccountService().setMatchRingBuffer(matchRingBuffer);
            dispatcher.getAccountService().setResponseRingBuffer(responseRingBuffer);
            dispatcher.setMatchServices(matchServices);
            accountServices.add(dispatcher.getAccountService());
        }
    }

    private List<AccountDispatcher> createAccountDispatchers() {
        List<AccountDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new AccountDispatcher(10, i));
        }
        return dispatchers;
    }

    private List<MatchDispatcher> createMatchDispatchers() {
        List<MatchDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new MatchDispatcher(10, i));
        }
        return dispatchers;
    }

    private AccountService getAccountService(int accountId) {
        return accountServices.get(accountId % 10);
    }

    /**
     * 处理系统繁忙的响应
     */
    private void handleSystemBusy(StreamObserver<?> responseObserver, String operationType) {
        switch (operationType) {
            case "PLACE_ORDER" -> {
                PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                        .setCode(RejectionReason.SYSTEM_BUSY.getCode())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<PlaceOrderResponse> observer = (StreamObserver<PlaceOrderResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "INCREASE" -> {
                IncreaseResponse response = IncreaseResponse.newBuilder()
                        .setCode(RejectionReason.SYSTEM_BUSY.getCode())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<IncreaseResponse> observer = (StreamObserver<IncreaseResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "DECREASE" -> {
                DecreaseResponse response = DecreaseResponse.newBuilder()
                        .setCode(RejectionReason.SYSTEM_BUSY.getCode())
                        .setMessage("System is busy, please try again later")
                        .build();
                @SuppressWarnings("unchecked")
                StreamObserver<DecreaseResponse> observer = (StreamObserver<DecreaseResponse>) responseObserver;
                observer.onNext(response);
                observer.onCompleted();
            }
            case "CANCEL_ORDER" -> {
                CancelOrderResponse response = CancelOrderResponse.newBuilder()
                        .setCode(RejectionReason.SYSTEM_BUSY.getCode())
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
        Map<Integer, SpotServiceProto.Balance> balances =
                getAccountService(request.getAccountId()).getBalances(request.getAccountId(), request.getCurrencyId()).entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    Balance balance = entry.getValue();
                                    return SpotServiceProto.Balance.newBuilder()
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
            observers.put(id, responseObserver);
            accountRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(id);
                message.type.set(EventType.INCREASE);
                Increase increase = message.payload.asIncrease;
                increase.accountId.set(request.getAccountId());
                increase.currencyId.set((short) request.getCurrencyId());
                increase.amount.set(currency.parse(request.getAmount()));
            });
        }, () -> {
            IncreaseResponse response = IncreaseResponse.newBuilder()
                    .setCode(RejectionReason.CURRENCY_NOT_EXIST.getCode())
                    .setMessage(RejectionReason.CURRENCY_NOT_EXIST.name())
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
            accountRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(id);
                message.type.set(EventType.DECREASE);
                Decrease decrease = message.payload.asDecrease;
                decrease.accountId.set(request.getAccountId());
                decrease.currencyId.set((short) request.getCurrencyId());
                decrease.amount.set(currency.parse(request.getAmount()));
            });
        }, () -> {
            DecreaseResponse response = DecreaseResponse.newBuilder()
                    .setCode(RejectionReason.CURRENCY_NOT_EXIST.getCode())
                    .setMessage(RejectionReason.CURRENCY_NOT_EXIST.name())
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
                message.id.set(id);
                message.type.set(EventType.PLACE_ORDER);
                PlaceOrder placeOrder = message.payload.asPlaceOrder;
                placeOrder.symbolId.set((short) request.getSymbolId());
                placeOrder.accountId.set(request.getAccountId());
                placeOrder.type.set(request.getType() == PlaceOrderRequest.Type.LIMIT ?
                        OrderType.LIMIT : OrderType.MARKET);
                placeOrder.side.set(request.getSide() == PlaceOrderRequest.Side.BID ?
                        OrderSide.BID : OrderSide.ASK);
                placeOrder.price.set(symbol.formatPrice(request.getPrice()));
                placeOrder.quantity.set(symbol.formatQuantity(request.getQuantity()));
                placeOrder.volume.set(symbol.formatPrice(request.getVolume()));
                placeOrder.takerRate.set(request.getTakerRate());
                placeOrder.makerRate.set(request.getMakerRate());
            });
        }, () -> {
            PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                    .setCode(RejectionReason.SYMBOL_NOT_EXIST.getCode())
                    .setMessage(RejectionReason.SYMBOL_NOT_EXIST.name())
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
            message.id.set(id);
            message.type.set(EventType.CANCEL_ORDER);
            message.payload.asCancelOrder.orderId.set(request.getOrderId());
        });
    }

    @Override
    public void getDepth(GetDepthRequest request, StreamObserver<GetDepthResponse> responseObserver) {
        int symbolId = request.getSymbolId();
        MatchService matchService = matchServices.get(symbolId % 10);
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
        memoryLeakDetector.stopMonitoring();
    }

    private class ResponseEventHandler implements EventHandler<Message>, LifecycleAware {

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(Message message, long sequence, boolean endOfBatch) {
            long id = message.id.get();
            if (id > 0) {
                // 获取observer并立即从Map中移除，防止内存泄漏
                StreamObserver<Object> observer = (StreamObserver<Object>) observers.remove(id);
                if (observer != null) {
                    EventType type = message.type.get();
                    Object response = switch (type) {
                        case ORDER_CREATED -> message.payload.asOrderCreated.get();
                        case PLACE_ORDER_REJECTED -> message.payload.asPlaceOrderRejected.get();
                        case ORDER_CANCELED -> message.payload.asOrderCanceled.get();
                        case INCREASED -> message.payload.asIncreased.get();
                        case DECREASED -> message.payload.asDecreased.get();
                        case DECREASE_REJECTED -> message.payload.asDecreaseRejected.get();
                        default -> throw new RuntimeException();
                    };
                    observer.onNext(response);
                    observer.onCompleted();
                }
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
