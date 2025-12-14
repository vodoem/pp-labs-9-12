package ru.rsreu;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TestClient {

    private final String clientId;
    private final List<OrderEvent> events = new CopyOnWriteArrayList<>();
    private Disposable subscription;

    public TestClient(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    public void subscribe(Flux<OrderEvent> stream) {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        subscription = stream.subscribe(events::add);
    }

    public List<OrderEvent> getEvents() {
        return events;
    }

    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
