package com.example.ops;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RateLimiterTest {
    @Test
    void doesNotDeadlockOrThrow_underBasicUse() {
        RateLimiter limiter = RateLimiter.create(50.0);
        assertTimeout(Duration.ofSeconds(2), () -> {
            for (int i = 0; i < 20; i++) {
                limiter.acquire();
            }
        });
    }

    @Test
    void enforcesApproximateRateOverTime_withTolerance() {
        RateLimiter limiter = RateLimiter.create(5.0); // 200ms/permit

        long start = System.nanoTime();
        limiter.acquire(); // immediate-ish
        limiter.acquire();
        limiter.acquire();
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        // Expected ~400ms. Use a tolerance to avoid flakiness from scheduling jitter.
        assertTrue(elapsedMs >= 250, "elapsedMs=" + elapsedMs);
    }

    @Test
    void tryAcquire_isNonBlocking_andEventuallyAllows() {
        RateLimiter limiter = RateLimiter.create(10.0); // 100ms/permit

        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire(), "second permit should be denied immediately");
        assertTimeout(Duration.ofSeconds(2), () -> {
            while (!limiter.tryAcquire()) {
                Thread.sleep(10);
            }
        });
    }
}

