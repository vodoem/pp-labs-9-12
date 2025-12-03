package ru.rsreu;

public final class Order {
    private final long id;
    private final OrderRequest request;
    private final long creationTimeNanos;
    private OrderStatus status;
    private long remainingQuantity;

    public Order(long id, OrderRequest request) {
        this.id = id;
        this.request = request;
        this.remainingQuantity = request.getQuantity();
        this.status = OrderStatus.NEW;
        this.creationTimeNanos = System.nanoTime();
    }

    public long getId() {
        return id;
    }

    public OrderRequest getRequest() {
        return request;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public long getRemainingQuantity() {
        return remainingQuantity;
    }

    public void decreaseRemaining(long delta) {
        if (delta <= 0 || delta > remainingQuantity) {
            throw new IllegalArgumentException("Invalid fill amount");
        }
        remainingQuantity -= delta;
    }

    public long getCreationTimeNanos() {
        return creationTimeNanos;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", clientId='" + request.getClientId() + '\'' +
                ", pair=" + request.getPair() +
                ", side=" + request.getSide() +
                ", limitPrice=" + request.getLimitPrice() +
                ", remainingQuantity=" + remainingQuantity +
                ", status=" + status +
                '}';
    }
}
