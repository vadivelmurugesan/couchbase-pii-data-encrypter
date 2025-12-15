package com.example.ops;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Minimal, threadsafe, fixed-rate limiter.
 * <p>
 * Each {@link #acquire()} reserves one permit. Callers will block until the permit is available.
 */
public final class RateLimiter {
    private final long intervalNanos;
    private final AtomicLong nextFreeNanos;

    private RateLimiter(long intervalNanos) {
        this.intervalNanos = Math.max(0, intervalNanos);
        this.nextFreeNanos = new AtomicLong(System.nanoTime());
    }

    public static RateLimiter create(double permitsPerSecond) {
        if (!(permitsPerSecond > 0.0) || Double.isNaN(permitsPerSecond) || Double.isInfinite(permitsPerSecond)) {
            throw new IllegalArgumentException("permitsPerSecond must be finite and > 0");
        }
        double interval = 1_000_000_000d / permitsPerSecond;
        long intervalNanos = interval < 1d ? 0L : (long) interval;
        return new RateLimiter(intervalNanos);
    }

    public static RateLimiter unlimited() {
        return new RateLimiter(0L);
    }

    public void acquire() {
        if (intervalNanos == 0) {
            return;
        }

        while (true) {
            long now = System.nanoTime();
            long currentNext = nextFreeNanos.get();
            long allowedAt = Math.max(now, currentNext);
            long newNext = allowedAt + intervalNanos;
            if (nextFreeNanos.compareAndSet(currentNext, newNext)) {
                long sleepNanos = allowedAt - now;
                if (sleepNanos > 0) {
                    LockSupport.parkNanos(sleepNanos);
                }
                return;
            }
        }
    }

    public boolean tryAcquire() {
        if (intervalNanos == 0) {
            return true;
        }

        long now = System.nanoTime();
        long currentNext = nextFreeNanos.get();
        if (now < currentNext) {
            return false;
        }
        return nextFreeNanos.compareAndSet(currentNext, now + intervalNanos);
    }

    public Duration interval() {
        return Duration.ofNanos(intervalNanos);
    }
}
