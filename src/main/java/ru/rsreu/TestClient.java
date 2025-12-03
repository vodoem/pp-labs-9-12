package ru.rsreu;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestClient implements ClientCallback {

    private final String clientId;
    private final BlockingQueue<OrderEvent> events = new LinkedBlockingQueue<>();

    public TestClient(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public void onEvent(OrderEvent event) {
        // Сохраняем событие, чтобы потом анализировать в main или в тестах
        events.add(event);
    }

    public BlockingQueue<OrderEvent> getEvents() {
        return events;
    }
}
