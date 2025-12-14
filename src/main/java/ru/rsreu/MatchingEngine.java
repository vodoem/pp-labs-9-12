package ru.rsreu;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MatchingEngine extends AutoCloseable {
    Mono<Void> registerClient(String clientId);

    Mono<Long> placeOrder(OrderRequest request);

    Mono<Void> cancelOrder(long orderId, String clientId);

    Mono<ExchangeSnapshot> snapshot();

    Flux<OrderEvent> events(String clientId);

    @Override
    default void close() throws Exception {
        // no-op by default
    }
}
