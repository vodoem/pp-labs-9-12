package ru.rsreu;

public final class Trade {
    private final long tradeId;
    private final long buyOrderId;
    private final long sellOrderId;
    private final CurrencyPair pair;
    private final long price;     // цена сделки
    private final long quantity;  // количество

    public Trade(long tradeId,
                 long buyOrderId,
                 long sellOrderId,
                 CurrencyPair pair,
                 long price,
                 long quantity) {
        this.tradeId = tradeId;
        this.buyOrderId = buyOrderId;
        this.sellOrderId = sellOrderId;
        this.pair = pair;
        this.price = price;
        this.quantity = quantity;
    }

    public long getTradeId() {
        return tradeId;
    }

    public long getBuyOrderId() {
        return buyOrderId;
    }

    public long getSellOrderId() {
        return sellOrderId;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public long getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "tradeId=" + tradeId +
                ", buyOrderId=" + buyOrderId +
                ", sellOrderId=" + sellOrderId +
                ", pair=" + pair +
                ", price=" + price +
                ", quantity=" + quantity +
                '}';
    }
}