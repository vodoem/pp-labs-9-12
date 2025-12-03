package ru.rsreu;

public interface MatchingEngine extends AutoCloseable {
    void registerClient(String clientId, ClientCallback callback);

    long placeOrder(OrderRequest request, ClientCallback callback);

    void cancelOrder(long orderId, String clientId, ClientCallback callback);

    ExchangeSnapshot snapshot();

    @Override
    default void close() throws Exception {
        // no-op by default
    }
}
