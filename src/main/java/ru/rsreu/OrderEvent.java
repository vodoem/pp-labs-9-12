package ru.rsreu;

public final class OrderEvent {
    private final long orderId;
    private final String clientId;
    private final ExecutionType type;
    private final CurrencyPair pair;
    private final Side side;
    private final String reason;       // для REJECTED/CANCELED
    private final Long tradePrice;     // null, если не сделка
    private final Long tradeQuantity;  // null, если не сделка
    private final Long counterOrderId; // null, если не сделка
    private final long filledQuantity; // суммарно исполнено по ордеру
    private final long remainingQuantity;

    public OrderEvent(long orderId,
                      String clientId,
                      ExecutionType type,
                      CurrencyPair pair,
                      Side side,
                      String reason,
                      Long tradePrice,
                      Long tradeQuantity,
                      Long counterOrderId,
                      long filledQuantity,
                      long remainingQuantity) {
        this.orderId = orderId;
        this.clientId = clientId;
        this.type = type;
        this.pair = pair;
        this.side = side;
        this.reason = reason;
        this.tradePrice = tradePrice;
        this.tradeQuantity = tradeQuantity;
        this.counterOrderId = counterOrderId;
        this.filledQuantity = filledQuantity;
        this.remainingQuantity = remainingQuantity;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getClientId() {
        return clientId;
    }

    public ExecutionType getType() {
        return type;
    }

    public CurrencyPair getPair() {
        return pair;
    }

    public Side getSide() {
        return side;
    }

    public String getReason() {
        return reason;
    }

    public Long getTradePrice() {
        return tradePrice;
    }

    public Long getTradeQuantity() {
        return tradeQuantity;
    }

    public Long getCounterOrderId() {
        return counterOrderId;
    }

    public long getFilledQuantity() {
        return filledQuantity;
    }

    public long getRemainingQuantity() {
        return remainingQuantity;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", clientId='" + clientId + '\'' +
                ", type=" + type +
                ", pair=" + pair +
                ", side=" + side +
                ", reason='" + reason + '\'' +
                ", tradePrice=" + tradePrice +
                ", tradeQuantity=" + tradeQuantity +
                ", counterOrderId=" + counterOrderId +
                ", filledQuantity=" + filledQuantity +
                ", remainingQuantity=" + remainingQuantity +
                '}';
    }
}
