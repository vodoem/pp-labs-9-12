package ru.rsreu;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws Exception {

        // 1. Создаём биржу
        Exchange exchange = new Exchange(
                List.of(new CurrencyPair(Currency.USD, Currency.RUB))
        );

        // 2. Регистрируем клиентов
        TestClient alice = new TestClient("alice");
        TestClient bob = new TestClient("bob");

        exchange.registerClient("alice", alice);
        exchange.registerClient("bob", bob);

        // 3. Отправляем ордера
        System.out.println("=== Размещение ордеров ===");

        exchange.placeOrder(
                new OrderRequest("alice",
                        new CurrencyPair(Currency.USD, Currency.RUB),
                        Side.BUY,
                        100,
                        80
                ),
                alice
        );

        exchange.placeOrder(
                new OrderRequest("bob",
                        new CurrencyPair(Currency.USD, Currency.RUB),
                        Side.SELL,
                        100,
                        75
                ),
                bob
        );

        // 4. Вывод нотификаций
        System.out.println("\n=== Нотификации Alice ===");
        alice.getEvents().forEach(System.out::println);

        System.out.println("\n=== Нотификации Bob ===");
        bob.getEvents().forEach(System.out::println);

        // 5. Snapshot биржи
        System.out.println("\n=== Snapshot ===");
        ExchangeSnapshot snapshot = exchange.snapshot();
        snapshot.getBooks().forEach((pair, book) -> {
            System.out.println(pair + ": bids=" + book.getBids().size() +
                    " asks=" + book.getAsks().size());
        });

        snapshot.getTrades().forEach(System.out::println);

        // 6. Стресс-тест
        System.out.println("\n=== Stress Test ===");
        stressTest(exchange);

        // 7. Final snapshot
        System.out.println("\n=== Final snapshot ===");
        ExchangeSnapshot after = exchange.snapshot();
        System.out.println("Trades: " + after.getTrades().size());
    }

    private static void stressTest(Exchange exchange) throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            pool.submit(() -> {
                String user = "U" + id;
                exchange.registerClient(user, new TestClient(user));
                CurrencyPair pair = new CurrencyPair(Currency.USD, Currency.RUB);

                for (int j = 0; j < 50; j++) {
                    exchange.placeOrder(
                            new OrderRequest(
                                    user,
                                    pair,
                                    (j % 2 == 0 ? Side.BUY : Side.SELL),
                                    10,
                                    70 + j % 5
                            ),
                            exchange.getCallback(user)
                    );
                }
                latch.countDown();
            });
        }

        latch.await();
        pool.shutdown();
    }
}
