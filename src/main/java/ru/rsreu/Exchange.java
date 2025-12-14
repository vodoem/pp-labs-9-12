package ru.rsreu;

import reactive.collections.ReactiveQueue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Exchange implements MatchingEngine {

    private final Map<CurrencyPair, OrderBook> books = new ConcurrentHashMap<>();
    private final Map<String, ReactiveQueue<OrderEvent>> events = new ConcurrentHashMap<>();

    private final AtomicLong orderIdSeq = new AtomicLong(1);
    private final AtomicLong tradeIdSeq = new AtomicLong(1);

    // Для snapshot'а истории сделок
    private final List<Trade> trades = new ArrayList<>();
    private final Object tradesLock = new Object();

    public Exchange(Collection<CurrencyPair> supportedPairs) {
        for (CurrencyPair pair : supportedPairs) {
            books.put(pair, new OrderBook(pair));
        }
    }

    private OrderBook getBookOrThrow(CurrencyPair pair) {
        OrderBook book = books.get(pair);
        if (book == null) {
            throw new IllegalArgumentException("Unsupported currency pair: " + pair);
        }
        return book;
    }

    @Override
    public Mono<Void> registerClient(String clientId) {
        return Mono.fromRunnable(() -> events.computeIfAbsent(clientId, id -> new ReactiveQueue<>()));
    }

    @Override
    public Flux<OrderEvent> events(String clientId) {
        ReactiveQueue<OrderEvent> queue = events.get(clientId);
        return queue == null ? Flux.empty() : queue.flux();
    }

    /**
     * Создание ордера. Вся обработка (matching) происходит синхронно в потоке вызывающего кода.
     */
    @Override
    public Mono<Long> placeOrder(OrderRequest request) {
        Objects.requireNonNull(request, "request");
        OrderBook book = getBookOrThrow(request.getPair());
        return Mono.fromCallable(() -> {
            book.getLock().lock();
            try {
                long orderId = orderIdSeq.getAndIncrement();
                Order order = new Order(orderId, request);

                emitEvent(request.getClientId(), new OrderEvent(
                        orderId,
                        request.getClientId(),
                        ExecutionType.ACCEPTED,
                        request.getPair(),
                        request.getSide(),
                        null,
                        null,
                        null,
                        null,
                        0,
                        order.getRemainingQuantity()
                ));

                match(order, book);

                if (order.getStatus() != OrderStatus.FILLED && order.getStatus() != OrderStatus.CANCELED) {
                    book.addOrderToBook(order);
                }

                return orderId;
            } catch (RuntimeException e) {
                emitEvent(request.getClientId(), new OrderEvent(
                        -1L,
                        request.getClientId(),
                        ExecutionType.REJECTED,
                        request.getPair(),
                        request.getSide(),
                        "Exception: " + e.getMessage(),
                        null,
                        null,
                        null,
                        0,
                        request.getQuantity()
                ));
                throw e;
            } finally {
                book.getLock().unlock();
            }
        });
    }

    /**
     * Отмена ордера по orderId.
     */
    @Override
    public Mono<Void> cancelOrder(long orderId, String clientId) {
        Objects.requireNonNull(clientId, "clientId");
        return Mono.fromRunnable(() -> {
            for (OrderBook book : books.values()) {
                book.getLock().lock();
                try {
                    Order order = book.getOrdersById().get(orderId);
                    if (order == null) {
                        continue;
                    }

                    if (order.getStatus() == OrderStatus.FILLED || order.getStatus() == OrderStatus.CANCELED) {
                        emitEvent(clientId, new OrderEvent(
                                orderId,
                                clientId,
                                ExecutionType.CANCELED,
                                order.getRequest().getPair(),
                                order.getRequest().getSide(),
                                "Already finished",
                                null,
                                null,
                                null,
                                order.getRequest().getQuantity() - order.getRemainingQuantity(),
                                order.getRemainingQuantity()
                        ));
                        return;
                    }

                    book.removeOrderFromBook(order);
                    order.setStatus(OrderStatus.CANCELED);

                    emitEvent(clientId, new OrderEvent(
                            orderId,
                            clientId,
                            ExecutionType.CANCELED,
                            order.getRequest().getPair(),
                            order.getRequest().getSide(),
                            "Canceled by client",
                            null,
                            null,
                            null,
                            order.getRequest().getQuantity() - order.getRemainingQuantity(),
                            order.getRemainingQuantity()
                    ));
                    return;
                } finally {
                    book.getLock().unlock();
                }
            }

            emitEvent(clientId, new OrderEvent(
                    orderId,
                    clientId,
                    ExecutionType.REJECTED,
                    null,
                    null,
                    "Order not found",
                    null,
                    null,
                    null,
                    0,
                    0
            ));
        });
    }

    /**
     * Matching в рамках одной книги.
     */
    private void match(Order incoming, OrderBook book) {
        PriorityQueue<Order> oppositeQueue =
                (incoming.getRequest().getSide() == Side.BUY) ? book.getAsks() : book.getBids();

        long totalFilledForIncoming = 0;

        while (incoming.getRemainingQuantity() > 0 && !oppositeQueue.isEmpty()) {
            Order bestOpposite = oppositeQueue.peek();
            if (!canMatch(incoming, bestOpposite)) {
                break;
            }

            long tradeQty = Math.min(incoming.getRemainingQuantity(), bestOpposite.getRemainingQuantity());
            long tradePrice = chooseTradePrice(incoming, bestOpposite);

            // обновляем ордера
            incoming.decreaseRemaining(tradeQty);
            bestOpposite.decreaseRemaining(tradeQty);
            totalFilledForIncoming += tradeQty;

            long tradeId = tradeIdSeq.getAndIncrement();
            Trade trade = new Trade(
                    tradeId,
                    incoming.getRequest().getSide() == Side.BUY ? incoming.getId() : bestOpposite.getId(),
                    incoming.getRequest().getSide() == Side.BUY ? bestOpposite.getId() : incoming.getId(),
                    book.getPair(),
                    tradePrice,
                    tradeQty
            );
            synchronized (tradesLock) {
                trades.add(trade);
            }

            // обновляем статусы
            updateStatusAfterFill(incoming);
            updateStatusAfterFill(bestOpposite);

            // уведомляем обе стороны
            OrderEvent eventIncoming = new OrderEvent(
                    incoming.getId(),
                    incoming.getRequest().getClientId(),
                    incoming.getStatus() == OrderStatus.FILLED ? ExecutionType.FULL_FILL : ExecutionType.PARTIAL_FILL,
                    incoming.getRequest().getPair(),
                    incoming.getRequest().getSide(),
                    null,
                    tradePrice,
                    tradeQty,
                    bestOpposite.getId(),
                    incoming.getRequest().getQuantity() - incoming.getRemainingQuantity(),
                    incoming.getRemainingQuantity()
            );
            emitEvent(incoming.getRequest().getClientId(), eventIncoming);

            OrderEvent eventCounterparty = new OrderEvent(
                    bestOpposite.getId(),
                    bestOpposite.getRequest().getClientId(),
                    bestOpposite.getRemainingQuantity() == 0
                            ? ExecutionType.FULL_FILL
                            : ExecutionType.PARTIAL_FILL,
                    bestOpposite.getRequest().getPair(),
                    bestOpposite.getRequest().getSide(),
                    null,
                    tradePrice,
                    tradeQty,
                    incoming.getId(),
                    bestOpposite.getRequest().getQuantity() - bestOpposite.getRemainingQuantity(),
                    bestOpposite.getRemainingQuantity()
            );

            emitEvent(bestOpposite.getRequest().getClientId(), eventCounterparty);
            if (bestOpposite.getRemainingQuantity() == 0) {
                oppositeQueue.poll();
                book.getOrdersById().remove(bestOpposite.getId());
            }
        }

        // Если ничего не сматчилось — статус NEW уже стоит
        if (totalFilledForIncoming > 0 && incoming.getRemainingQuantity() > 0) {
            incoming.setStatus(OrderStatus.PARTIALLY_FILLED);
        }
    }

    private void updateStatusAfterFill(Order order) {
        if (order.getRemainingQuantity() == 0) {
            order.setStatus(OrderStatus.FILLED);
        } else if (order.getRemainingQuantity() < order.getRequest().getQuantity()) {
            order.setStatus(OrderStatus.PARTIALLY_FILLED);
        }
    }

    private boolean canMatch(Order incoming, Order opposite) {
        long buyPrice = (incoming.getRequest().getSide() == Side.BUY)
                ? incoming.getRequest().getLimitPrice()
                : opposite.getRequest().getLimitPrice();
        long sellPrice = (incoming.getRequest().getSide() == Side.SELL)
                ? incoming.getRequest().getLimitPrice()
                : opposite.getRequest().getLimitPrice();
        return buyPrice >= sellPrice;
    }

    private long chooseTradePrice(Order incoming, Order opposite) {
        // Примитивно: цена ордера, который был в книге (taker платит цену maker'a)
        return opposite.getRequest().getLimitPrice();
    }

    /**
     * Полный снимок состояния биржи.
     * Предполагается вызывать, когда активных операций нет (после теста).
     */
    @Override
    public Mono<ExchangeSnapshot> snapshot() {
        return Mono.fromSupplier(this::buildSnapshot);
    }

    private ExchangeSnapshot buildSnapshot() {
        Map<CurrencyPair, OrderBookSnapshot> bookSnapshots = new HashMap<>();

        for (OrderBook book : books.values()) {
            book.getLock().lock();
            try {
                List<OrderView> bids = new ArrayList<>();
                for (Order o : book.getBids()) {
                    bids.add(new OrderView(
                            o.getId(),
                            o.getRequest().getClientId(),
                            o.getRequest().getSide(),
                            o.getRequest().getLimitPrice(),
                            o.getRemainingQuantity(),
                            o.getStatus()
                    ));
                }

                List<OrderView> asks = new ArrayList<>();
                for (Order o : book.getAsks()) {
                    asks.add(new OrderView(
                            o.getId(),
                            o.getRequest().getClientId(),
                            o.getRequest().getSide(),
                            o.getRequest().getLimitPrice(),
                            o.getRemainingQuantity(),
                            o.getStatus()
                    ));
                }

                bookSnapshots.put(book.getPair(), new OrderBookSnapshot(book.getPair(), bids, asks));
            } finally {
                book.getLock().unlock();
            }
        }

        List<Trade> tradesCopy;
        synchronized (tradesLock) {
            tradesCopy = new ArrayList<>(trades);
        }

        return new ExchangeSnapshot(bookSnapshots, tradesCopy);
    }

    private void emitEvent(String clientId, OrderEvent event) {
        events.computeIfAbsent(clientId, id -> new ReactiveQueue<>()).emit(event);
    }
}