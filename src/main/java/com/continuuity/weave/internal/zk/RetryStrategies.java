package com.continuuity.weave.internal.zk;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class RetryStrategies {

  public static RetryStrategy noRetry() {
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime, OperationType type, String path) {
        return -1;
      }
    };
  }

  public static RetryStrategy limit(final int limit, final RetryStrategy strategy) {
    Preconditions.checkArgument(limit >= 0, "limit must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime, OperationType type, String path) {
        return (failureCount <= limit) ? strategy.nextRetry(failureCount, startTime, type, path) : -1L;
      }
    };
  }

  public static RetryStrategy fixDelay(final long delay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(delay >= 0, "delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime, OperationType type, String path) {
        return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
      }
    };
  }

  public static RetryStrategy exponentialDelay(final long baseDelay, final long maxDelay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(baseDelay >= 0, "base delay must be >= 0");
    Preconditions.checkArgument(maxDelay >= 0, "max delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime, OperationType type, String path) {
        long power = failureCount > Long.SIZE ? Long.MAX_VALUE : (1L << (failureCount - 1));
        long delay = Math.min(baseDelay * power, maxDelay);
        delay = delay < 0 ? maxDelay : delay;
        return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
      }
    };
  }

  public static RetryStrategy timeLimit(long maxElapseTime, TimeUnit timeUnit, final RetryStrategy strategy) {
    Preconditions.checkArgument(maxElapseTime >= 0, "max elapse time must be >= 0");
    final long maxElapseMs = TimeUnit.MILLISECONDS.convert(maxElapseTime, timeUnit);
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, long startTime, OperationType type, String path) {
        long elapseTime = System.currentTimeMillis() - startTime;
        return elapseTime <= maxElapseMs ? strategy.nextRetry(failureCount, startTime, type, path) : -1L;
      }
    };
  }

  private RetryStrategies() {
  }
}
