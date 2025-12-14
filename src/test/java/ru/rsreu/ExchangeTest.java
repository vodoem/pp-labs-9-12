package ru.rsreu;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ExchangeTest {

    private static final CurrencyPair PAIR = new CurrencyPair(Currency.USD, Currency.RUB);

    static Stream<EngineCase> engines() {
        return Stream.of(
                new EngineCase("Sync Exchange", () -> new Exchange(List.of(PAIR))),
                new EngineCase("Queued Exchange", () -> new QueuedExchange(List.of(PAIR)))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void matchesOrdersAndCleansOrderBook(EngineCase engineCase) {
        try (MatchingEngine exchange = engineCase.factory().get()) {
            TestClient buyer = new TestClient("buyer");
            TestClient seller = new TestClient("seller");

            exchange.registerClient(buyer.getClientId()).block();
            exchange.registerClient(seller.getClientId()).block();
            buyer.subscribe(exchange.events(buyer.getClientId()));
            seller.subscribe(exchange.events(seller.getClientId()));

            exchange.placeOrder(new OrderRequest(
                            buyer.getClientId(),
                            PAIR,
                            Side.BUY,
                            10,
                            100
                    )
            ).block();

            exchange.placeOrder(new OrderRequest(
                            seller.getClientId(),
                            PAIR,
                            Side.SELL,
                            10,
                            95
                    )
            ).block();

            ExchangeSnapshot snapshot = exchange.snapshot().block();
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void keepsPartiallyFilledOrdersInBook(EngineCase engineCase) {
        try (MatchingEngine exchange = engineCase.factory().get()) {
            TestClient buyer = new TestClient("partial-buyer");
            TestClient seller = new TestClient("partial-seller");

            exchange.registerClient(buyer.getClientId()).block();
            exchange.registerClient(seller.getClientId()).block();
            buyer.subscribe(exchange.events(buyer.getClientId()));
            seller.subscribe(exchange.events(seller.getClientId()));

            exchange.placeOrder(new OrderRequest(
                            buyer.getClientId(),
                            PAIR,
                            Side.BUY,
                            10,
                            100
                    )
            ).block();

            exchange.placeOrder(new OrderRequest(
                            seller.getClientId(),
                            PAIR,
                            Side.SELL,
                            5,
                            95
                    )
            ).block();

            ExchangeSnapshot snapshot = exchange.snapshot().block();
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void stressTestPreservesMoneyBalances(EngineCase engineCase) throws Exception {
        try (MatchingEngine exchange = engineCase.factory().get()) {
            Map<String, TestClient> clients = new ConcurrentHashMap<>();

            int threads = 8;
            int ordersPerThread = 60;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);

            List<Runnable> registrations = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                final int idx = i;
                registrations.add(() -> {
                    String clientId = "U" + idx;
                    TestClient client = new TestClient(clientId);
                    clients.put(clientId, client);
                    exchange.registerClient(clientId).block();
                    client.subscribe(exchange.events(clientId));

                    for (int j = 0; j < ordersPerThread; j++) {
                        Side side = (j % 2 == 0) ? Side.BUY : Side.SELL;
                        long price = (side == Side.BUY) ? 101 : 99;
                        exchange.placeOrder(new OrderRequest(
                                        clientId,
                                        PAIR,
                                        side,
                                        5,
                                        price
                                )
                        ).block();
                    }
                    latch.countDown();
                });
            }

            for (Runnable task : registrations) {
                executor.submit(task);
            }

            latch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            List<OrderEvent> allEvents = clients.values().stream()
                    .map(TestClient::getEvents)
                    .flatMap(Collection::stream)
                    .toList();

            Map<Currency, Long> balanceChanges = calculateBalances(allEvents);
            assertEquals(0L, balanceChanges.getOrDefault(Currency.USD, 0L), "USD balance should stay constant");
            assertEquals(0L, balanceChanges.getOrDefault(Currency.RUB, 0L), "RUB balance should stay constant");

            ExchangeSnapshot snapshot = exchange.snapshot().block();
            assertFalse(snapshot.getTrades().isEmpty(), "stress test should produce trades");
        }
    }

    @Test
    void comparesThroughput() {
        EngineCase sync = new EngineCase("Sync", () -> new Exchange(List.of(PAIR)));
        EngineCase async = new EngineCase("Queued", () -> new QueuedExchange(List.of(PAIR)));

        double syncRps = measureThroughput(sync.factory(), 2_000);
        double asyncRps = measureThroughput(async.factory(), 2_000);

        assertTrue(syncRps > 0, "sync throughput should be positive");
        assertTrue(asyncRps > 0, "async throughput should be positive");
        System.out.printf("Throughput sync=%.2f rps, async=%.2f rps%n", syncRps, asyncRps);
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

    private double measureThroughput(Supplier<MatchingEngine> factory, int totalOrders) {
        long start = System.nanoTime();
        try (MatchingEngine exchange = factory.get()) {
            TestClient buyer = new TestClient("perf-buyer");
            TestClient seller = new TestClient("perf-seller");
            exchange.registerClient(buyer.getClientId()).block();
            exchange.registerClient(seller.getClientId()).block();
            buyer.subscribe(exchange.events(buyer.getClientId()));
            seller.subscribe(exchange.events(seller.getClientId()));

            for (int i = 0; i < totalOrders; i++) {
                Side side = (i % 2 == 0) ? Side.BUY : Side.SELL;
                TestClient client = side == Side.BUY ? buyer : seller;
                exchange.placeOrder(new OrderRequest(
                                client.getClientId(),
                                PAIR,
                                side,
                                1,
                                side == Side.BUY ? 101 : 99
                        )
                ).block();
            }

            exchange.snapshot().block();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long nanos = System.nanoTime() - start;
        double seconds = nanos / 1_000_000_000.0;
        return totalOrders / seconds;
    }

    private record EngineCase(String name, Supplier<MatchingEngine> factory) {
        @Override
        public String toString() {
            return name;
        }
    }
}
