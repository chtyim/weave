package com.continuuity.weave.internal.zk;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class RetryStrategies {

  public static RetryStrategy retryN(final int n, final long delay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(delay >= 0, "delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, OperationType type, String path) {
        if (failureCount <= n) {
          return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
        }
        return -1;
      }
    };
  }

  public static RetryStrategy retryUnlimited(final long delay, final TimeUnit delayUnit) {
    Preconditions.checkArgument(delay >= 0, "delay must be >= 0");
    return new RetryStrategy() {
      @Override
      public long nextRetry(int failureCount, OperationType type, String path) {
        return TimeUnit.MILLISECONDS.convert(delay, delayUnit);
      }
    };
  }

  private RetryStrategies() {
  }
}
