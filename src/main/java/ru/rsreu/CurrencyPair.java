package ru.rsreu;

import java.util.Objects;

public final class CurrencyPair {
    private final Currency base;
    private final Currency quote;

    public CurrencyPair(Currency base, Currency quote) {
        if (base == null || quote == null) {
            throw new IllegalArgumentException("Currencies must not be null");
        }
        this.base = base;
        this.quote = quote;
    }

    public Currency getBase() {
        return base;
    }

    public Currency getQuote() {
        return quote;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrencyPair)) return false;
        CurrencyPair that = (CurrencyPair) o;
        return base == that.base && quote == that.quote;
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, quote);
    }

    @Override
    public String toString() {
        return base + "/" + quote;
    }
}
