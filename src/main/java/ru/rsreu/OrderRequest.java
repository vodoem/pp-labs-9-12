package ru.rsreu;

public final class OrderRequest {
    private final String clientId;
    private final CurrencyPair pair;
    private final Side side;
    private final long quantity;    // количество базовой валюты
    private final long limitPrice;  // цена в котируемой валюте за 1 единицу базовой

    public OrderRequest(String clientId,
                        CurrencyPair pair,
                        Side side,
                        long quantity,
                        long limitPrice) {
        if (clientId == null || clientId.isEmpty()) {
            throw new IllegalArgumentException("clientId must not be empty");
        }
        if (pair == null || side == null) {
            throw new IllegalArgumentException("pair and side must not be null");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
        if (limitPrice <= 0) {
            throw new IllegalArgumentException("limitPrice must be > 0");
        }

        this.clientId = clientId;
        this.pair = pair;
        this.side = side;
        this.quantity = quantity;
        this.limitPrice = limitPrice;
    }

    public String getClientId() {
        return clientId;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public Side getSide() {
        return side;
    }

    public long getQuantity() {
        return quantity;
    }

    public long getLimitPrice() {
        return limitPrice;
    }
}
