package ru.rsreu;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExchangeTest {

    private static final CurrencyPair PAIR = new CurrencyPair(Currency.USD, Currency.RUB);

    @Test
    void matchesOrdersAndCleansOrderBook() {
        Exchange exchange = new Exchange(List.of(PAIR));
        TestClient buyer = new TestClient("buyer");
        TestClient seller = new TestClient("seller");

        exchange.registerClient(buyer.getClientId(), buyer);
        exchange.registerClient(seller.getClientId(), seller);

        exchange.placeOrder(new OrderRequest(
                        buyer.getClientId(),
                        PAIR,
                        Side.BUY,
                        10,
                        100
                ),
                buyer);

        exchange.placeOrder(new OrderRequest(
                        seller.getClientId(),
                        PAIR,
                        Side.SELL,
                        10,
                        95
                ),
                seller);

        ExchangeSnapshot snapshot = exchange.snapshot();
        OrderBookSnapshot bookSnapshot = snapshot.getBooks().get(PAIR);

        assertNotNull(bookSnapshot);
        assertTrue(bookSnapshot.getBids().isEmpty(), "all buy orders should be matched");
        assertTrue(bookSnapshot.getAsks().isEmpty(), "all sell orders should be matched");

        List<OrderEvent> buyerEvents = new ArrayList<>(buyer.getEvents());
        List<OrderEvent> sellerEvents = new ArrayList<>(seller.getEvents());

        OrderEvent buyerFill = buyerEvents.stream()
                .filter(e -> e.getType() == ExecutionType.FULL_FILL)
                .findFirst()
                .orElseThrow();
        OrderEvent sellerFill = sellerEvents.stream()
                .filter(e -> e.getType() == ExecutionType.FULL_FILL)
                .findFirst()
                .orElseThrow();

        assertEquals(0, buyerFill.getRemainingQuantity());
        assertEquals(0, sellerFill.getRemainingQuantity());
        assertEquals(10, buyerFill.getFilledQuantity());
        assertEquals(10, sellerFill.getFilledQuantity());
    }

    @Test
    void keepsPartiallyFilledOrdersInBook() {
        Exchange exchange = new Exchange(List.of(PAIR));
        TestClient buyer = new TestClient("partial-buyer");
        TestClient seller = new TestClient("partial-seller");

        exchange.registerClient(buyer.getClientId(), buyer);
        exchange.registerClient(seller.getClientId(), seller);

        exchange.placeOrder(new OrderRequest(
                        buyer.getClientId(),
                        PAIR,
                        Side.BUY,
                        10,
                        100
                ),
                buyer);

        exchange.placeOrder(new OrderRequest(
                        seller.getClientId(),
                        PAIR,
                        Side.SELL,
                        5,
                        95
                ),
                seller);

        ExchangeSnapshot snapshot = exchange.snapshot();
        OrderBookSnapshot bookSnapshot = snapshot.getBooks().get(PAIR);

        assertNotNull(bookSnapshot);
        assertEquals(1, bookSnapshot.getBids().size(), "partially filled buy should remain in book");
        OrderView remainingOrder = bookSnapshot.getBids().getFirst();
        assertEquals(5, remainingOrder.getRemainingQuantity());
        assertEquals(OrderStatus.PARTIALLY_FILLED, remainingOrder.getStatus());

        List<OrderEvent> buyerEvents = new ArrayList<>(buyer.getEvents());
        OrderEvent partialFill = buyerEvents.stream()
                .filter(e -> e.getType() == ExecutionType.PARTIAL_FILL)
                .findFirst()
                .orElseThrow();

        assertEquals(5, partialFill.getRemainingQuantity());
        assertEquals(5, partialFill.getFilledQuantity());
    }

    @Test
    void stressTestPreservesMoneyBalances() throws Exception {
        Exchange exchange = new Exchange(List.of(PAIR));
        Map<String, TestClient> clients = new ConcurrentHashMap<>();

        int threads = 8;
        int ordersPerThread = 60;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            Future<?> future = executor.submit(() -> {
                String clientId = "U" + idx;
                TestClient client = new TestClient(clientId);
                clients.put(clientId, client);
                exchange.registerClient(clientId, client);

                for (int j = 0; j < ordersPerThread; j++) {
                    Side side = (j % 2 == 0) ? Side.BUY : Side.SELL;
                    long price = (side == Side.BUY) ? 101 : 99;
                    exchange.placeOrder(new OrderRequest(
                                    clientId,
                                    PAIR,
                                    side,
                                    5,
                                    price
                            ),
                            client);
                }
                latch.countDown();
            });
            futures.add(future);
        }

        latch.await(5, TimeUnit.SECONDS);
        for (Future<?> future : futures) {
            future.get(1, TimeUnit.SECONDS);
        }
        executor.shutdown();

        List<OrderEvent> allEvents = clients.values().stream()
                .map(TestClient::getEvents)
                .flatMap(Collection::stream)
                .toList();

        Map<Currency, Long> balanceChanges = calculateBalances(allEvents);
        assertEquals(0L, balanceChanges.getOrDefault(Currency.USD, 0L), "USD balance should stay constant");
        assertEquals(0L, balanceChanges.getOrDefault(Currency.RUB, 0L), "RUB balance should stay constant");

        ExchangeSnapshot snapshot = exchange.snapshot();
        assertFalse(snapshot.getTrades().isEmpty(), "stress test should produce trades");
    }

    private Map<Currency, Long> calculateBalances(Collection<OrderEvent> events) {
        Map<Currency, Long> deltas = new ConcurrentHashMap<>();

        for (OrderEvent event : events) {
            if (event.getTradeQuantity() == null || event.getTradePrice() == null) {
                continue;
            }

            long quantity = event.getTradeQuantity();
            long price = event.getTradePrice();

            adjustBalance(deltas, PAIR.base(), event.getSide() == Side.BUY ? quantity : -quantity);
            adjustBalance(deltas, PAIR.quote(), event.getSide() == Side.BUY ? -quantity * price : quantity * price);
        }
        return deltas;
    }

    private void adjustBalance(Map<Currency, Long> balances, Currency currency, long delta) {
        balances.compute(currency, (key, value) -> value == null ? delta : value + delta);
    }
}
