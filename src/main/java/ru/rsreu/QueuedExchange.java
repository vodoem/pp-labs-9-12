package ru.rsreu;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class QueuedExchange implements MatchingEngine {

    private final BlockingQueue<Command<?>> queue = new LinkedBlockingQueue<>();
    private final Thread workerThread;
    private final AtomicLong orderIdSeq = new AtomicLong(1L);

    public QueuedExchange(Collection<CurrencyPair> supportedPairs) {
        ExchangeState state = new ExchangeState(supportedPairs);
        ExchangeWorker worker = new ExchangeWorker(queue, state);
        this.workerThread = new Thread(worker, "exchange-worker");
        this.workerThread.setDaemon(true);
        this.workerThread.start();
    }

    @Override
    public void registerClient(String clientId, ClientCallback callback) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(callback, "callback");
        RegisterClientCommand command = new RegisterClientCommand(clientId, callback);
        enqueue(command);
    }

    @Override
    public long placeOrder(OrderRequest request, ClientCallback callback) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(callback, "callback");
        long orderId = orderIdSeq.getAndIncrement();
        PlaceOrderCommand command = new PlaceOrderCommand(orderId, request, callback);
        enqueue(command);
        return orderId;
    }

    @Override
    public void cancelOrder(long orderId, String clientId, ClientCallback callback) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(callback, "callback");
        CancelOrderCommand command = new CancelOrderCommand(orderId, clientId, callback);
        enqueue(command);
    }

    @Override
    public ExchangeSnapshot snapshot() {
        SnapshotCommand command = new SnapshotCommand();
        return enqueueAndWait(command);
    }

    @Override
    public void close() throws InterruptedException {
        StopCommand stop = new StopCommand();
        enqueueAndWait(stop);
        workerThread.join(1000);
    }

    private void enqueue(Command<?> command) {
        try {
            queue.put(command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing command", e);
        }
    }

    private <T> T enqueueAndWait(Command<T> command) {
        try {
            queue.put(command);
            return command.result().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing command", e);
        }
    }

    private static final class ExchangeState {
        private final Map<CurrencyPair, OrderBook> books = new HashMap<>();
        private final Map<String, ClientCallback> callbacks = new HashMap<>();
        private final List<Trade> trades = new ArrayList<>();
        private long tradeIdSeq = 1L;

        ExchangeState(Collection<CurrencyPair> supportedPairs) {
            for (CurrencyPair pair : supportedPairs) {
                books.put(pair, new OrderBook(pair));
            }
        }

        OrderBook getBook(CurrencyPair pair) {
            OrderBook book = books.get(pair);
            if (book == null) {
                throw new IllegalArgumentException("Unsupported currency pair: " + pair);
            }
            return book;
        }
    }

    private static final class ExchangeWorker implements Runnable {
        private final BlockingQueue<Command<?>> queue;
        private final ExchangeState state;

        ExchangeWorker(BlockingQueue<Command<?>> queue, ExchangeState state) {
            this.queue = queue;
            this.state = state;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Command<?> command = queue.take();
                    if (command instanceof StopCommand) {
                        command.result().complete(null);
                        break;
                    }
                    try {
                        command.execute(state);
                    } catch (Exception e) {
                        command.result().completeExceptionally(e);
                        continue;
                    }
                    command.complete();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private abstract static class Command<T> {
        private final CompletableFuture<T> result = new CompletableFuture<>();

        abstract void execute(ExchangeState state);

        abstract T extractResult();

        void complete() {
            result.complete(extractResult());
        }

        CompletableFuture<T> result() {
            return result;
        }
    }

    private static final class RegisterClientCommand extends Command<Void> {
        private final String clientId;
        private final ClientCallback callback;

        RegisterClientCommand(String clientId, ClientCallback callback) {
            this.clientId = clientId;
            this.callback = callback;
        }

        @Override
        void execute(ExchangeState state) {
            state.callbacks.put(clientId, callback);
        }

        @Override
        Void extractResult() {
            return null;
        }
    }

    private static final class CancelOrderCommand extends Command<Void> {
        private final long orderId;
        private final String clientId;
        private final ClientCallback callback;

        CancelOrderCommand(long orderId, String clientId, ClientCallback callback) {
            this.orderId = orderId;
            this.clientId = clientId;
            this.callback = callback;
        }

        @Override
        void execute(ExchangeState state) {
            for (OrderBook book : state.books.values()) {
                Optional<Order> maybeOrder = book.getOrder(orderId);
                if (maybeOrder.isEmpty()) {
                    continue;
                }
                Order order = maybeOrder.get();
                if (order.getStatus() == OrderStatus.FILLED || order.getStatus() == OrderStatus.CANCELED) {
                    callback.onEvent(new OrderEvent(
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
                callback.onEvent(new OrderEvent(
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
            }

            callback.onEvent(new OrderEvent(
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
        }

        @Override
        Void extractResult() {
            return null;
        }
    }

    private static final class PlaceOrderCommand extends Command<Long> {
        private final long orderId;
        private final OrderRequest request;
        private final ClientCallback callback;

        PlaceOrderCommand(long orderId, OrderRequest request, ClientCallback callback) {
            this.orderId = orderId;
            this.request = request;
            this.callback = callback;
        }

        @Override
        void execute(ExchangeState state) {
            OrderBook book = state.getBook(request.getPair());
            Order order = new Order(orderId, request);

            callback.onEvent(new OrderEvent(
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

            match(order, book, state, callback);

            if (order.getStatus() != OrderStatus.FILLED && order.getStatus() != OrderStatus.CANCELED) {
                book.addOrderToBook(order);
            }
        }

        @Override
        Long extractResult() {
            return orderId;
        }
    }

    private static final class SnapshotCommand extends Command<ExchangeSnapshot> {
        private ExchangeSnapshot snapshot;

        @Override
        void execute(ExchangeState state) {
            Map<CurrencyPair, OrderBookSnapshot> snapshots = new HashMap<>();
            for (OrderBook book : state.books.values()) {
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

                snapshots.put(book.getPair(), new OrderBookSnapshot(book.getPair(), bids, asks));
            }
            snapshot = new ExchangeSnapshot(snapshots, new ArrayList<>(state.trades));
        }

        @Override
        ExchangeSnapshot extractResult() {
            return snapshot;
        }
    }

    private static final class StopCommand extends Command<Void> {
        @Override
        void execute(ExchangeState state) {
            // nothing to do
        }

        @Override
        Void extractResult() {
            return null;
        }
    }

    private static void match(Order incoming, OrderBook book, ExchangeState state, ClientCallback incomingCallback) {
        var oppositeQueue = (incoming.getRequest().getSide() == Side.BUY) ? book.getAsks() : book.getBids();

        long totalFilledForIncoming = 0;

        while (incoming.getRemainingQuantity() > 0 && !oppositeQueue.isEmpty()) {
            Order bestOpposite = oppositeQueue.peek();
            if (!canMatch(incoming, bestOpposite)) {
                break;
            }

            long tradeQty = Math.min(incoming.getRemainingQuantity(), bestOpposite.getRemainingQuantity());
            long tradePrice = chooseTradePrice(incoming, bestOpposite);

            incoming.decreaseRemaining(tradeQty);
            bestOpposite.decreaseRemaining(tradeQty);
            totalFilledForIncoming += tradeQty;

            long tradeId = state.tradeIdSeq++;
            Trade trade = new Trade(
                    tradeId,
                    incoming.getRequest().getSide() == Side.BUY ? incoming.getId() : bestOpposite.getId(),
                    incoming.getRequest().getSide() == Side.BUY ? bestOpposite.getId() : incoming.getId(),
                    book.getPair(),
                    tradePrice,
                    tradeQty
            );
            state.trades.add(trade);

            updateStatusAfterFill(incoming);
            updateStatusAfterFill(bestOpposite);

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
            incomingCallback.onEvent(eventIncoming);

            ClientCallback counterCb = state.callbacks.get(bestOpposite.getRequest().getClientId());
            if (counterCb != null) {
                OrderEvent eventCounterparty = new OrderEvent(
                        bestOpposite.getId(),
                        bestOpposite.getRequest().getClientId(),
                        bestOpposite.getRemainingQuantity() == 0 ? ExecutionType.FULL_FILL : ExecutionType.PARTIAL_FILL,
                        bestOpposite.getRequest().getPair(),
                        bestOpposite.getRequest().getSide(),
                        null,
                        tradePrice,
                        tradeQty,
                        incoming.getId(),
                        bestOpposite.getRequest().getQuantity() - bestOpposite.getRemainingQuantity(),
                        bestOpposite.getRemainingQuantity()
                );
                counterCb.onEvent(eventCounterparty);
            }
            if (bestOpposite.getRemainingQuantity() == 0) {
                oppositeQueue.poll();
                book.getOrdersById().remove(bestOpposite.getId());
            }
        }

        if (totalFilledForIncoming > 0 && incoming.getRemainingQuantity() > 0) {
            incoming.setStatus(OrderStatus.PARTIALLY_FILLED);
        }
    }

    private static void updateStatusAfterFill(Order order) {
        if (order.getRemainingQuantity() == 0) {
            order.setStatus(OrderStatus.FILLED);
        } else if (order.getRemainingQuantity() < order.getRequest().getQuantity()) {
            order.setStatus(OrderStatus.PARTIALLY_FILLED);
        }
    }

    private static boolean canMatch(Order incoming, Order opposite) {
        long buyPrice = (incoming.getRequest().getSide() == Side.BUY) ? incoming.getRequest().getLimitPrice() : opposite.getRequest().getLimitPrice();
        long sellPrice = (incoming.getRequest().getSide() == Side.SELL) ? incoming.getRequest().getLimitPrice() : opposite.getRequest().getLimitPrice();
        return buyPrice >= sellPrice;
    }

    private static long chooseTradePrice(Order incoming, Order opposite) {
        return opposite.getRequest().getLimitPrice();
    }
}
