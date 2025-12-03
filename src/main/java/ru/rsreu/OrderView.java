package ru.rsreu;

public final class OrderView {
    private final long orderId;
    private final String clientId;
    private final Side side;
    private final long limitPrice;
    private final long remainingQuantity;
    private final OrderStatus status;

    public OrderView(long orderId,
                     String clientId,
                     Side side,
                     long limitPrice,
                     long remainingQuantity,
                     OrderStatus status) {
        this.orderId = orderId;
        this.clientId = clientId;
        this.side = side;
        this.limitPrice = limitPrice;
        this.remainingQuantity = remainingQuantity;
        this.status = status;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getClientId() {
        return clientId;
    }

    public Side getSide() {
        return side;
    }

    public long getLimitPrice() {
        return limitPrice;
    }

    public long getRemainingQuantity() {
        return remainingQuantity;
    }

    public OrderStatus getStatus() {
        return status;
    }
}
