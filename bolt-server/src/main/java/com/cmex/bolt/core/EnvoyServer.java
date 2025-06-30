package com.cmex.bolt.core;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.domain.Balance;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.Envoy.*;
import com.cmex.bolt.EnvoyServerGrpc;
import com.cmex.bolt.handler.SequencerDispatcher;
import com.cmex.bolt.handler.MatchDispatcher;
import com.cmex.bolt.monitor.RingBufferMonitor;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.service.AccountService;
import com.cmex.bolt.service.MatchService;
import com.cmex.bolt.util.BackpressureManager;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.SystemBusyResponseFactory;
import com.cmex.bolt.util.SystemBusyResponses;

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

    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    private final RingBuffer<NexusWrapper> matchingRingBuffer;
    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    private final List<AccountService> accountServices;
    private final List<MatchService> matchServices;

    // 背压管理器
    private final BackpressureManager sequencerBackpressureManager;
    private final BackpressureManager matchBackpressureManager;
    private final BackpressureManager responseBackpressureManager;
    private final RingBufferMonitor ringBufferMonitor;
    private final Transfer transfer = new Transfer();

    //分组数量
    private final int group;

    public EnvoyServer(BoltConfig boltConfig) {
        this.group = boltConfig.group();
        observers = new ConcurrentHashMap<>();
        WaitStrategy waitStrategy;
        if (boltConfig.isProd()) {
            waitStrategy = new BusySpinWaitStrategy();
        } else {
            waitStrategy = new BusySpinWaitStrategy();
        }
        Disruptor<NexusWrapper> sequencerDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), boltConfig.sequencerSize(), DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, waitStrategy);
        Disruptor<NexusWrapper> matchingDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), boltConfig.matchingSize(), DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, waitStrategy);
        Disruptor<NexusWrapper> responseDisruptor =
                new Disruptor<>(new NexusWrapper.Factory(256), boltConfig.responseSize(), DaemonThreadFactory.INSTANCE,
                        ProducerType.MULTI, waitStrategy);// Response通常是单生产者

        List<SequencerDispatcher> sequencerDispatchers = createSequencerDispatchers();
        List<MatchDispatcher> matchDispatchers = createMatchingDispatchers();
        matchingDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        sequencerDisruptor.handleEventsWith(sequencerDispatchers.toArray(new SequencerDispatcher[0]));

        sequencerRingBuffer = sequencerDisruptor.start();
        matchingRingBuffer = matchingDisruptor.start();
        RingBuffer<NexusWrapper> responseRingBuffer = responseDisruptor.start();

        // 初始化背压管理器
        sequencerBackpressureManager = new BackpressureManager("Sequencer", sequencerRingBuffer);
        matchBackpressureManager = new BackpressureManager("Match", matchingRingBuffer);
        responseBackpressureManager = new BackpressureManager("Response", responseRingBuffer);

        // 初始化监控器
        ringBufferMonitor = new RingBufferMonitor(5000); // 5秒报告一次
        ringBufferMonitor.addManager(sequencerBackpressureManager);
        ringBufferMonitor.addManager(matchBackpressureManager);
        ringBufferMonitor.addManager(responseBackpressureManager);
        ringBufferMonitor.startMonitoring();

        matchServices = new ArrayList<>(group);
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getMatchService().setSequencerRingBuffer(sequencerRingBuffer);
            dispatcher.getMatchService().setResponseRingBuffer(responseRingBuffer);
            matchServices.add(dispatcher.getMatchService());
        }
        accountServices = new ArrayList<>(group);
        for (SequencerDispatcher dispatcher : sequencerDispatchers) {
            dispatcher.getAccountService().setMatchingRingBuffer(matchingRingBuffer);
            dispatcher.getAccountService().setResponseRingBuffer(responseRingBuffer);
            accountServices.add(dispatcher.getAccountService());
        }
    }

    private List<SequencerDispatcher> createSequencerDispatchers() {
        List<SequencerDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < group; i++) {
            dispatchers.add(new SequencerDispatcher(group, i));
        }
        return dispatchers;
    }

    private List<MatchDispatcher> createMatchingDispatchers() {
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
     * 背压检查
     */
    private <T> boolean handleBackpressure(StreamObserver<T> responseObserver,
                                           SystemBusyResponseFactory<T> responseFactory,
                                           BackpressureManager backpressureManager) {
        BackpressureManager.BackpressureResult result = backpressureManager.checkCapacity();

        if (result == BackpressureManager.BackpressureResult.CRITICAL_REJECT) {
            sendResponse(responseObserver, responseFactory.createResponse());
            return true;
        }

        if (result == BackpressureManager.BackpressureResult.HIGH_LOAD) {
            System.out.printf("WARNING: Account RingBuffer usage is high: %.2f%%%n",
                    backpressureManager.getCurrentUsageRate() * 100);
        }
        return false;
    }

    /**
     * 通用响应发送方法
     */
    private <T> void sendResponse(StreamObserver<T> responseObserver, T response) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
        // 背压检查
        if (handleBackpressure(responseObserver, SystemBusyResponses::createIncreaseBusyResponse,
                sequencerBackpressureManager)) {
            return;
        }

        getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId()).ifPresentOrElse(currency -> {
            long id = requestId.incrementAndGet();
            int partition = getPartition(request.getAccountId());
            observers.put(id, responseObserver);
            sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                wrapper.setPartition(partition);
                transfer.writeIncreaseRequest(request, currency, wrapper.getBuffer());
            });
        }, () -> {
            IncreaseResponse response = IncreaseResponse.newBuilder()
                    .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                    .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                    .build();
            sendResponse(responseObserver, response);
        });
    }

    public void decrease(DecreaseRequest request, StreamObserver<DecreaseResponse> responseObserver) {
        // 背压检查
        if (handleBackpressure(responseObserver, SystemBusyResponses::createDecreaseBusyResponse,
                sequencerBackpressureManager)) {
            return;
        }

        getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId())
                .ifPresentOrElse(currency -> {
                    long id = requestId.incrementAndGet();
                    int partition = getPartition(request.getAccountId());
                    observers.put(id, responseObserver);
                    sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                        wrapper.setId(id);
                        wrapper.setPartition(partition);
                        transfer.writeDecreaseRequest(request, currency, wrapper.getBuffer());
                    });
                }, () -> {
                    DecreaseResponse response = DecreaseResponse.newBuilder()
                            .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                            .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                            .build();
                    sendResponse(responseObserver, response);
                });
    }

    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        if (handleBackpressure(responseObserver, SystemBusyResponses::createPlaceOrderBusyResponse,
                sequencerBackpressureManager)) {
            return;
        }

        //TODO
        //市价单 支持买金额卖数量
        getSymbol(request.getSymbolId()).ifPresentOrElse(symbol -> {
            long id = requestId.incrementAndGet();
            int partition = getPartition(request.getAccountId());
            observers.put(id, responseObserver);
            sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                wrapper.setPartition(partition);
                transfer.writePlaceOrderRequest(request, symbol, wrapper.getBuffer());
            });
        }, () -> {
            PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                    .setCode(Nexus.RejectionReason.SYMBOL_NOT_EXIST.ordinal())
                    .setMessage(Nexus.RejectionReason.SYMBOL_NOT_EXIST.name())
                    .build();
            sendResponse(responseObserver, response);
        });
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        matchingRingBuffer.publishEvent((wrapper, sequence) -> {
            wrapper.setId(id);
            wrapper.setPartition(OrderIdGenerator.getSymbolId(request.getOrderId()) % group);
            transfer.writeCancelOrderRequest(request, wrapper.getBuffer());
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
    public BackpressureManager.BackpressureStats getSequencerBackpressureStats() {
        return sequencerBackpressureManager.getStats();
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
        sequencerBackpressureManager.resetStats();
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