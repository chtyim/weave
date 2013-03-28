package com.continuuity.weave.internal.zk;

/**
 * Provides static factory method to create {@link ZKClientService} with modified behaviors.
 */
public final class ZKClientServices {

  /**
   * Creates a {@link ZKClientService} that will perform auto re-watch on all existing watches
   * when reconnection happens after session expiration. All {@link org.apache.zookeeper.Watcher Watchers}
   * set through the returned {@link ZKClientService} would not receive any connection events.
   *
   * @param clientService The {@link ZKClientService} for operations delegation.
   * @return A {@link ZKClientService} that will do auto re-watch on all methods that accept a
   *        {@link org.apache.zookeeper.Watcher} upon session expiration.
   */
  public static ZKClientService reWatchOnExpire(ZKClientService clientService) {
    return new RewatchOnExpireZKClientService(clientService);
  }

  /**
   * Creates a {@link ZKClientService} that will retry failure operations based on the given {@link RetryStrategy}.
   *
   * @param clientService The {@link ZKClientService} for operations delegation.
   * @param retryStrategy The {@link RetryStrategy} to be invoke when there is operation failure.
   * @return A {@link ZKClientService}.
   */
  public static ZKClientService retryOnFailure(ZKClientService clientService, RetryStrategy retryStrategy) {
    return new FailureRetryZKClientService(clientService, retryStrategy);
  }

  private ZKClientServices() {
  }
}
