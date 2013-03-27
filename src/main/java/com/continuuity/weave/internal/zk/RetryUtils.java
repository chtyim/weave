package com.continuuity.weave.internal.zk;

import org.apache.zookeeper.KeeperException;

/**
 *
 */
final class RetryUtils {

  public static boolean canRetry(KeeperException.Code code) {
    return (code == KeeperException.Code.CONNECTIONLOSS
          || code == KeeperException.Code.OPERATIONTIMEOUT
          || code == KeeperException.Code.SESSIONEXPIRED
          || code == KeeperException.Code.SESSIONMOVED);
  }

  public static boolean canRetry(Throwable t) {
    if (!(t instanceof KeeperException)) {
      return false;
    }
    return canRetry(((KeeperException)t).code());
  }

  private RetryUtils() {
  }
}
