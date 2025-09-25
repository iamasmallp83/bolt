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
import com.cmex.bolt.handler.JournalHandler;
import com.cmex.bolt.handler.JournalReplayer;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.service.AccountService;
import com.cmex.bolt.service.MatchService;
import com.cmex.bolt.util.PerformanceExporter;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.SystemBusyResponseFactory;
import com.cmex.bolt.util.SystemBusyResponses;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class EnvoyServer extends EnvoyServerGrpc.EnvoyServerImplBase {

    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    private final RingBuffer<NexusWrapper> matchingRingBuffer;
    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    private final List<AccountService> accountServices;
    private final List<MatchService> matchServices;

    // 性能导出器
    @Getter
    private final PerformanceExporter performanceExporter;

    private final Transfer transfer;

    //分组数量
    private final int group;

    public EnvoyServer(BoltConfig boltConfig) {
        this.group = boltConfig.group();
        observers = new ConcurrentHashMap<>();
        WaitStrategy waitStrategy;
        if (boltConfig.isProd()) {
            waitStrategy = new BusySpinWaitStrategy();
        } else {
            waitStrategy = new BlockingWaitStrategy();
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
        
        JournalHandler journalHandler = new JournalHandler(boltConfig);

        matchingDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        
        // 配置 sequencerRingBuffer 的事件处理链：JournalHandler -> SequencerDispatcher
        sequencerDisruptor.handleEventsWith(journalHandler)
                .then(sequencerDispatchers.toArray(new SequencerDispatcher[0]));

        sequencerRingBuffer = sequencerDisruptor.start();
        matchingRingBuffer = matchingDisruptor.start();
        RingBuffer<NexusWrapper> responseRingBuffer = responseDisruptor.start();

        // 初始化性能导出器
        performanceExporter = new PerformanceExporter(sequencerRingBuffer);
        
        // 如果存在 journal 文件，则进行重放
        JournalReplayer replayer = new JournalReplayer(sequencerRingBuffer, boltConfig);
        replayer.replayFromJournal();

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
        transfer = new Transfer();
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

            getAccountService(request.getAccountId()).getCurrency(request.getCurrencyId()).ifPresentOrElse(currency -> {
                long id = requestId.incrementAndGet();
                int partition = getPartition(request.getAccountId());
                observers.put(id, responseObserver);
                sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
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
            long id = requestId.incrementAndGet();
            observers.put(id, responseObserver);
            matchingRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(id);
                wrapper.setPartition(OrderIdGenerator.getSymbolId(request.getOrderId()) % group);
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
            MatchService matchService = matchServices.get(symbolId % group);
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
        return matchServices.get(symbolId % group).getSymbol(symbolId);
    }
    
}