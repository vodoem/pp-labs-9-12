package ru.rsreu;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;

/**
 * Реализация MatchingEngine, выполняющая все операции в выделенном потоке,
 * но предоставляющая реактивный интерфейс Mono/Flux.
 */
public final class QueuedExchange implements MatchingEngine {

    private final Exchange delegate;
    private final Scheduler scheduler;

    public QueuedExchange(Collection<CurrencyPair> supportedPairs) {
        this.delegate = new Exchange(supportedPairs);
        this.scheduler = Schedulers.newSingle("exchange-worker");
    }

    @Override
    public Mono<Void> registerClient(String clientId) {
        return delegate.registerClient(clientId).subscribeOn(scheduler);
    }

    @Override
    public Flux<OrderEvent> events(String clientId) {
        return delegate.events(clientId);
    }

    @Override
    public Mono<Long> placeOrder(OrderRequest request) {
        return delegate.placeOrder(request).subscribeOn(scheduler);
    }

    @Override
    public Mono<Void> cancelOrder(long orderId, String clientId) {
        return delegate.cancelOrder(orderId, clientId).subscribeOn(scheduler);
    }

    @Override
    public Mono<ExchangeSnapshot> snapshot() {
        return delegate.snapshot().subscribeOn(scheduler);
    }

    @Override
    public void close() {
        scheduler.dispose();
    }
}
