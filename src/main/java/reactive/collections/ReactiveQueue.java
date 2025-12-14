package reactive.collections;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Простая реактивная очередь на базе {@link Sinks.Many}, позволяющая
 * отправлять события и подписываться на них как на Flux.
 */
public class ReactiveQueue<T> {

    private final Sinks.Many<T> sink = Sinks.many().multicast().onBackpressureBuffer();

    public Flux<T> flux() {
        return sink.asFlux();
    }

    public void emit(T item) {
        sink.tryEmitNext(item);
    }
}
