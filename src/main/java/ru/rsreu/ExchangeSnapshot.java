package ru.rsreu;

import java.util.List;
import java.util.Map;

public final class ExchangeSnapshot {
    private final Map<CurrencyPair, OrderBookSnapshot> books;
    private final List<Trade> trades;

    public ExchangeSnapshot(Map<CurrencyPair, OrderBookSnapshot> books,
                            List<Trade> trades) {
        this.books = books;
        this.trades = trades;
    }

    public Map<CurrencyPair, OrderBookSnapshot> getBooks() {
        return books;
    }

    public List<Trade> getTrades() {
        return trades;
    }
}
