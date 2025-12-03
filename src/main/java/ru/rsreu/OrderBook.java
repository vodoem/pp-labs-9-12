package ru.rsreu;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class OrderBook {
    private final CurrencyPair pair;
    private final Lock lock = new ReentrantLock();

    // Лучшие buy — с максимальной ценой
    private final PriorityQueue<Order> bids =
            new PriorityQueue<>(Comparator
                    .comparingLong((Order o) -> o.getRequest().getLimitPrice()).reversed()
                    .thenComparingLong(Order::getCreationTimeNanos));

    // Лучшие sell — с минимальной ценой
    private final PriorityQueue<Order> asks =
            new PriorityQueue<>(Comparator
                    .comparingLong((Order o) -> o.getRequest().getLimitPrice())
                    .thenComparingLong(Order::getCreationTimeNanos));

    // Для snapshot'а и поиска при cancel
    private final Map<Long, Order> ordersById = new HashMap<>();

    OrderBook(CurrencyPair pair) {
        this.pair = pair;
    }

    CurrencyPair getPair() {
        return pair;
    }

    Lock getLock() {
        return lock;
    }

    Optional<Order> getOrder(long orderId) {
        return Optional.ofNullable(ordersById.get(orderId));
    }

    void addOrderToBook(Order order) {
        ordersById.put(order.getId(), order);
        if (order.getRequest().getSide() == Side.BUY) {
            bids.add(order);
        } else {
            asks.add(order);
        }
    }

    void removeOrderFromBook(Order order) {
        ordersById.remove(order.getId());
        if (order.getRequest().getSide() == Side.BUY) {
            bids.remove(order);
        } else {
            asks.remove(order);
        }
    }

    PriorityQueue<Order> getBids() {
        return bids;
    }

    PriorityQueue<Order> getAsks() {
        return asks;
    }

    Map<Long, Order> getOrdersById() {
        return ordersById;
    }
}
