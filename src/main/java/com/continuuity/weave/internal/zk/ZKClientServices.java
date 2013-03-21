package com.continuuity.weave.internal.zk;

/**
 *
 */
public final class ZKClientServices {

  /**
   * Creates a {@link ZKClientService} that will performs auto re-watch on all existing watches
   * when reconnection happens after session expiration.
   *
   * @param clientService
   * @param retryStrategy
   * @return A {@link ZKClientService} that will do auto re-watch on all methods that accept a
   *        {@link org.apache.zookeeper.Watcher} upon session expiration. Also the given {@link RetryStrategy}
   *        will be used for operation retry.
   */
  public static ZKClientService reWatchOnExpire(ZKClientService clientService, RetryStrategy retryStrategy) {
    return new ExpireRewatchZKClientService(retryOnFailure(clientService, retryStrategy));
  }

  public static ZKClientService retryOnFailure(ZKClientService clientService, RetryStrategy retryStrategy) {
    return new FailureRetryZKClientService(clientService, retryStrategy);
  }

  private ZKClientServices() {
  }
}
