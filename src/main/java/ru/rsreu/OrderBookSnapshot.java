package ru.rsreu;

import java.util.List;

public final class OrderBookSnapshot {
    private final CurrencyPair pair;
    private final List<OrderView> bids;
    private final List<OrderView> asks;

    public OrderBookSnapshot(CurrencyPair pair,
                             List<OrderView> bids,
                             List<OrderView> asks) {
        this.pair = pair;
        this.bids = bids;
        this.asks = asks;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public List<OrderView> getBids() {
        return bids;
    }

    public List<OrderView> getAsks() {
        return asks;
    }
}
