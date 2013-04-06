package com.continuuity.internal.zk;

import org.apache.zookeeper.KeeperException;

/**
 * Utility class for help determining operation retry condition.
 */
final class RetryUtils {

  /**
   * Tells if a given operation error code can be retried or not.
   * @param code The error code of the operation.
   * @return {@code true} if the operation can be retried.
   */
  public static boolean canRetry(KeeperException.Code code) {
    return (code == KeeperException.Code.CONNECTIONLOSS
          || code == KeeperException.Code.OPERATIONTIMEOUT
          || code == KeeperException.Code.SESSIONEXPIRED
          || code == KeeperException.Code.SESSIONMOVED);
  }

  /**
   * Tells if a given operation exception can be retried or not.
   * @param t The exception raised by an operation.
   * @return {@code true} if the operation can be retried.
   */
  public static boolean canRetry(Throwable t) {
    if (!(t instanceof KeeperException)) {
      return false;
    }
    return canRetry(((KeeperException)t).code());
  }

  private RetryUtils() {
  }
}
