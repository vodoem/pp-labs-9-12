package ru.rsreu;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws Exception {

        Exchange exchange = new Exchange(
                List.of(new CurrencyPair(Currency.USD, Currency.RUB))
        );

        TestClient alice = new TestClient("alice");
        TestClient bob = new TestClient("bob");

        exchange.registerClient("alice").block();
        exchange.registerClient("bob").block();
        alice.subscribe(exchange.events("alice"));
        bob.subscribe(exchange.events("bob"));

        System.out.println("=== Размещение ордеров ===");

        exchange.placeOrder(
                new OrderRequest("alice",
                        new CurrencyPair(Currency.USD, Currency.RUB),
                        Side.BUY,
                        100,
                        80
                )
        ).block();

        exchange.placeOrder(
                new OrderRequest("bob",
                        new CurrencyPair(Currency.USD, Currency.RUB),
                        Side.SELL,
                        100,
                        75
                )
        ).block();

        System.out.println("\n=== Нотификации Alice ===");
        alice.getEvents().forEach(System.out::println);

        System.out.println("\n=== Нотификации Bob ===");
        bob.getEvents().forEach(System.out::println);

        System.out.println("\n=== Snapshot ===");
        ExchangeSnapshot snapshot = exchange.snapshot().block();
        snapshot.getBooks().forEach((pair, book) -> {
            System.out.println(pair + ": bids=" + book.getBids().size() +
                    " asks=" + book.getAsks().size());
        });

        snapshot.getTrades().forEach(System.out::println);

        System.out.println("\n=== Stress Test ===");
        stressTest(exchange);

        System.out.println("\n=== Final snapshot ===");
        ExchangeSnapshot after = exchange.snapshot().block();
        System.out.println("Trades: " + after.getTrades().size());

        alice.stop();
        bob.stop();
    }

    private static void stressTest(Exchange exchange) throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            pool.submit(() -> {
                String user = "U" + id;
                exchange.registerClient(user).block();
                TestClient client = new TestClient(user);
                client.subscribe(exchange.events(user));
                CurrencyPair pair = new CurrencyPair(Currency.USD, Currency.RUB);

                for (int j = 0; j < 50; j++) {
                    exchange.placeOrder(
                            new OrderRequest(
                                    user,
                                    pair,
                                    (j % 2 == 0 ? Side.BUY : Side.SELL),
                                    10,
                                    70 + j % 5
                            )
                    ).block();
                }
                latch.countDown();
            });
        }

        latch.await();
        pool.shutdown();
    }
}
