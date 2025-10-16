package com.cmex.bolt.core;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Envoy.*;
import com.cmex.bolt.EnvoyServerGrpc;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.domain.Balance;
import com.cmex.bolt.domain.Symbol;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.handler.*;
import com.cmex.bolt.recovery.SnapshotData;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.service.AccountService;
import com.cmex.bolt.service.MatchService;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.PerformanceExporter;
import com.cmex.bolt.util.SystemBusyResponseFactory;
import com.cmex.bolt.util.SystemBusyResponses;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class EnvoyServer extends EnvoyServerGrpc.EnvoyServerImplBase {
    private static final Logger log = LoggerFactory.getLogger(EnvoyServer.class);

    private final BoltConfig config;

    // Disruptor实例，用于关闭
    private final Disruptor<NexusWrapper> sequencerDisruptor;
    private final Disruptor<NexusWrapper> matchingDisruptor;
    private final Disruptor<NexusWrapper> responseDisruptor;

    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    private List<AccountService> accountServices;
    private List<MatchService> matchServices;

    // 性能导出器
    @Getter
    private PerformanceExporter performanceExporter;

    private final Transfer transfer;

    // 复制相关组件（仅主节点使用）
    @Getter
    private ReplicationHandler replicationHandler;

    // 公共构造函数
    public EnvoyServer(BoltConfig config, SnapshotData snapshotData) {
        this.config = config;
        this.transfer = new Transfer();
        log.info("EnvoyServer constructor called with config: {}", config);

        // 3. 创建Disruptor RingBuffers
        observers = new ConcurrentHashMap<>();
        WaitStrategy waitStrategy = config.isProd() ? new BusySpinWaitStrategy() : new BlockingWaitStrategy();

        this.sequencerDisruptor = new Disruptor<>(
                new NexusWrapper.Factory(256), config.sequencerSize(), DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI, waitStrategy);
        this.matchingDisruptor = new Disruptor<>(
                new NexusWrapper.Factory(256), config.matchingSize(), DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI, waitStrategy);
        this.responseDisruptor = new Disruptor<>(
                new NexusWrapper.Factory(256), config.responseSize(), DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI, waitStrategy);

        // 4. 创建Dispatchers
        List<AccountDispatcher> accountDispatchers = createAccountDispatchers(snapshotData);
        List<MatchDispatcher> matchDispatchers = createMatchDispatchers(snapshotData);

        // 5. 配置处理链
        configureProcessingChains(sequencerDisruptor, matchingDisruptor, responseDisruptor,
                accountDispatchers, matchDispatchers);

        // 6. 启动RingBuffers
        sequencerDisruptor.start();
        matchingDisruptor.start();
        responseDisruptor.start();


        // 8. 初始化服务
        initializeServices(accountDispatchers, matchDispatchers);

        // 9. 初始化性能导出器
        performanceExporter = new PerformanceExporter(sequencerDisruptor.getRingBuffer());

        log.info("EnvoyServer initialized successfully");
    }

    /**
     * 关闭EnvoyServer
     */
    public void shutdown() {
        log.info("Shutting down EnvoyServer");

        // 关闭Disruptor实例
        if (sequencerDisruptor != null) {
            sequencerDisruptor.shutdown();
        }
        if (matchingDisruptor != null) {
            matchingDisruptor.shutdown();
        }
        if (responseDisruptor != null) {
            responseDisruptor.shutdown();
        }

        // 关闭性能导出器
        if (performanceExporter != null) {
            performanceExporter.shutdown();
        }

        log.info("EnvoyServer shutdown completed");
    }

    // Getter方法
    public RingBuffer<NexusWrapper> getSequencerRingBuffer() {
        return sequencerDisruptor.getRingBuffer();
    }

    public RingBuffer<NexusWrapper> getMatchingRingBuffer() {
        return matchingDisruptor.getRingBuffer();
    }

    public RingBuffer<NexusWrapper> getResponseRingBuffer() {
        return responseDisruptor.getRingBuffer();
    }

    /**
     * 重放Journal数据
     */
    public void replayJournal() {
        if (config.isMaster()) {
            // 主节点：从本地journal重放
            log.info("Master node: replaying from local journal");
            JournalReplayer replayer = new JournalReplayer(config, getSequencerRingBuffer());
            long maxId = replayer.replayFromJournal();
            requestId.set(maxId);
        }
    }

    /**
     * 创建AccountDispatchers
     */
    private List<AccountDispatcher> createAccountDispatchers(SnapshotData snapshotData) {
        List<AccountDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < config.group(); i++) {
            try {
                AccountRepository accountRepository = snapshotData.accountRepositories().get(i);
                CurrencyRepository currencyRepository = snapshotData.currencyRepositories().get(i);
                SymbolRepository symbolRepository = snapshotData.symbolRepositories().get(i);

                dispatchers.add(new AccountDispatcher(config, config.group(), i,
                        accountRepository, currencyRepository, symbolRepository));
            } catch (Exception e) {
                log.error("Failed to create AccountDispatcher for partition {}", i, e);
                throw new RuntimeException(e);
            }
        }
        return dispatchers;
    }

    /**
     * 创建MatchDispatchers
     */
    private List<MatchDispatcher> createMatchDispatchers(SnapshotData snapshotData) {
        List<MatchDispatcher> dispatchers = new ArrayList<>();

        for (int i = 0; i < config.group(); i++) {
            try {
                SymbolRepository symbolRepository = snapshotData.symbolRepositories().get(i);
                dispatchers.add(new MatchDispatcher(config, config.group(), i, symbolRepository));
            } catch (Exception e) {
                log.error("Failed to create MatchDispatcher for partition {}", i, e);
                throw new RuntimeException(e);
            }
        }

        return dispatchers;
    }

    /**
     * 配置处理链
     */
    private void configureProcessingChains(Disruptor<NexusWrapper> sequencerDisruptor,
                                           Disruptor<NexusWrapper> matchingDisruptor,
                                           Disruptor<NexusWrapper> responseDisruptor,
                                           List<AccountDispatcher> accountDispatchers,
                                           List<MatchDispatcher> matchDispatchers) {

        if (config.isMaster()) {
            // 主节点：JournalHandler -> ReplicationHandler -> AccountDispatchers
            JournalHandler journalHandler = new JournalHandler(config);
            this.replicationHandler = new ReplicationHandler(config, getSequencerRingBuffer());

            sequencerDisruptor.handleEventsWith(journalHandler)
                    .then(replicationHandler)
                    .then(accountDispatchers.toArray(new AccountDispatcher[0]));
        } else {
            // 从节点：直接到AccountDispatchers（无需Journal和Replication）
            this.replicationHandler = null;
            sequencerDisruptor.handleEventsWith(accountDispatchers.toArray(new AccountDispatcher[0]));
        }

        // Matching处理链（主从节点相同）
        matchingDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));

        // Response处理链（主从节点相同）
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
    }

    /**
     * 初始化服务
     */
    private void initializeServices(List<AccountDispatcher> accountDispatchers,
                                    List<MatchDispatcher> matchDispatchers) {
        matchServices = new ArrayList<>(config.group());
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getMatchService().setSequencerRingBuffer(sequencerDisruptor.getRingBuffer());
            dispatcher.getMatchService().setResponseRingBuffer(responseDisruptor.getRingBuffer());
            matchServices.add(dispatcher.getMatchService());
        }

        accountServices = new ArrayList<>(config.group());
        for (AccountDispatcher dispatcher : accountDispatchers) {
            dispatcher.getAccountService().setMatchingRingBuffer(sequencerDisruptor.getRingBuffer());
            dispatcher.getAccountService().setResponseRingBuffer(responseDisruptor.getRingBuffer());
            dispatcher.setMatchingRingBuffer(matchingDisruptor.getRingBuffer());
            accountServices.add(dispatcher.getAccountService());
        }
    }

    private AccountService getAccountService(int accountId) {
        return accountServices.get(getPartition(accountId));
    }

    private int getPartition(int accountId) {
        return accountId % config.group();
    }

    /**
     * 背压检查
     */
    private <T> boolean handleBackpressure(StreamObserver<T> responseObserver,
                                           SystemBusyResponseFactory<T> responseFactory,
                                           PerformanceExporter performanceExporter) {
        boolean canAccept = performanceExporter.checkCapacity();

        if (!canAccept) {
            sendResponse(responseObserver, responseFactory.createResponse());
            return true;
        }

        if (performanceExporter.isHighLoad()) {
            System.out.printf("WARNING: Account RingBuffer usage is high: %.2f%%%n",
                    performanceExporter.getCurrentUsageRate() * 100);
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
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("getAccount")) {
            Map<Integer, Envoy.Balance> balances =
                    getAccountService(request.getAccountId()).getBalances(request.getAccountId(), request.getCurrencyId()).entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> {
                                        Balance balance = entry.getValue();
                                        return Envoy.Balance.newBuilder()
                                                .setCurrency(balance.getCurrency().getName())
                                                .setFrozen(balance.getFrozen().toString())
                                                .setAvailable(balance.available().toString())
                                                .setValue(balance.getValue().toString())
                                                .build();
                                    }));
            GetAccountResponse response = GetAccountResponse.newBuilder()
                    .setCode(1)
                    .putAllData(balances)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            timer.recordSuccess();
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("getAccount", false, 0.0);
            responseObserver.onError(e);
        }
    }

    public void increase(IncreaseRequest request, StreamObserver<IncreaseResponse> responseObserver) {
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("increase")) {
            // 背压检查
            if (handleBackpressure(responseObserver, SystemBusyResponses::createIncreaseBusyResponse,
                    performanceExporter)) {
                timer.recordError();
                return;
            }
            if (config.isSlave()) {
                IncreaseResponse response = IncreaseResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.NOT_SUPPORTED.ordinal())
                        .setMessage(Nexus.RejectionReason.NOT_SUPPORTED.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            }
            getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId()).ifPresentOrElse(currency -> {
                long id = requestId.incrementAndGet();
                int partition = getPartition(request.getAccountId());
                observers.put(id, responseObserver);
                getSequencerRingBuffer().publishEvent((wrapper, sequence) -> {
                    wrapper.setId(id);
                    wrapper.setPartition(partition);
                    transfer.writeIncreaseRequest(request, currency, wrapper.getBuffer());
                });
                timer.recordSuccess();
            }, () -> {
                IncreaseResponse response = IncreaseResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                        .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            });
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("increase", false, 0.0);
            responseObserver.onError(e);
        }
    }

    public void decrease(DecreaseRequest request, StreamObserver<DecreaseResponse> responseObserver) {
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("decrease")) {
            // 背压检查
            if (handleBackpressure(responseObserver, SystemBusyResponses::createDecreaseBusyResponse,
                    performanceExporter)) {
                timer.recordError();
                return;
            }
            if (config.isSlave()) {
                DecreaseResponse response = DecreaseResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.NOT_SUPPORTED.ordinal())
                        .setMessage(Nexus.RejectionReason.NOT_SUPPORTED.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            }

            getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId())
                    .ifPresentOrElse(currency -> {
                        long id = requestId.incrementAndGet();
                        int partition = getPartition(request.getAccountId());
                        observers.put(id, responseObserver);
                        sequencerDisruptor.getRingBuffer().publishEvent((wrapper, sequence) -> {
                            wrapper.setId(id);
                            wrapper.setPartition(partition);
                            transfer.writeDecreaseRequest(request, currency, wrapper.getBuffer());
                        });
                        timer.recordSuccess();
                    }, () -> {
                        DecreaseResponse response = DecreaseResponse.newBuilder()
                                .setCode(Nexus.RejectionReason.CURRENCY_NOT_EXIST.ordinal())
                                .setMessage(Nexus.RejectionReason.CURRENCY_NOT_EXIST.name())
                                .build();
                        sendResponse(responseObserver, response);
                        timer.recordError();
                    });
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("decrease", false, 0.0);
            responseObserver.onError(e);
        }
    }

    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("placeOrder")) {
            if (handleBackpressure(responseObserver, SystemBusyResponses::createPlaceOrderBusyResponse,
                    performanceExporter)) {
                timer.recordError();
                return;
            }

            if (config.isSlave()) {
                PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.NOT_SUPPORTED.ordinal())
                        .setMessage(Nexus.RejectionReason.NOT_SUPPORTED.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            }

            //TODO
            //市价单 支持买金额卖数量
            getSymbol(request.getSymbolId()).ifPresentOrElse(symbol -> {
                long id = requestId.incrementAndGet();
                int partition = getPartition(request.getAccountId());
                observers.put(id, responseObserver);
                sequencerDisruptor.getRingBuffer().publishEvent((wrapper, sequence) -> {
                    wrapper.setId(id);
                    wrapper.setPartition(partition);
                    transfer.writePlaceOrderRequest(request, symbol, wrapper.getBuffer());
                });
                timer.recordSuccess();
            }, () -> {
                PlaceOrderResponse response = PlaceOrderResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.SYMBOL_NOT_EXIST.ordinal())
                        .setMessage(Nexus.RejectionReason.SYMBOL_NOT_EXIST.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            });
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("placeOrder", false, 0.0);
            responseObserver.onError(e);
        }
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> responseObserver) {
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("cancelOrder")) {
            if (handleBackpressure(responseObserver, SystemBusyResponses::createCancelOrderBusyResponse,
                    performanceExporter)) {
                timer.recordError();
                return;
            }
            if (config.isSlave()) {
                CancelOrderResponse response = CancelOrderResponse.newBuilder()
                        .setCode(Nexus.RejectionReason.NOT_SUPPORTED.ordinal())
                        .setMessage(Nexus.RejectionReason.NOT_SUPPORTED.name())
                        .build();
                sendResponse(responseObserver, response);
                timer.recordError();
            }
            long id = requestId.incrementAndGet();
            observers.put(id, responseObserver);
            sequencerDisruptor.getRingBuffer().publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                wrapper.setPartition(OrderIdGenerator.getSymbolId(request.getOrderId()) % config.group());
                transfer.writeCancelOrderRequest(request, wrapper.getBuffer());
            });
            timer.recordSuccess();
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("cancelOrder", false, 0.0);
            responseObserver.onError(e);
        }
    }

    @Override
    public void getDepth(Envoy.GetDepthRequest request, StreamObserver<Envoy.GetDepthResponse> responseObserver) {
        try (PerformanceExporter.GrpcTimer timer = performanceExporter.createGrpcTimer("getDepth")) {
            int symbolId = request.getSymbolId();
            MatchService matchService = matchServices.get(symbolId % config.group());
            DepthDto dto = matchService.getDepth(symbolId);
            GetDepthResponse response = GetDepthResponse.newBuilder()
                    .setCode(1)
                    .setData(Depth.newBuilder().setSymbol(dto.getSymbol()).putAllAsks(dto.getAsks()).putAllBids(dto.getBids()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            timer.recordSuccess();
        } catch (Exception e) {
            performanceExporter.recordGrpcCall("getDepth", false, 0.0);
            responseObserver.onError(e);
        }
    }

    private class ResponseEventHandler implements EventHandler<NexusWrapper>, LifecycleAware {

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
            if (config.isMaster() && (wrapper.isBusinessEvent() || wrapper.isInternalEvent())) {
                StreamObserver<Object> observer = (StreamObserver<Object>) observers.get(wrapper.getId());
                //todo 如何处理 new CurrencyRepository()
                Object object = transfer.to(new CurrencyRepository(), wrapper.getBuffer());
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
        return matchServices.get(symbolId % config.group()).getSymbol(symbolId);
    }

}