package com.continuuity.weave.internal.zk;

/**
 *
 */
public interface RetryStrategy {

  enum OperationType {
    CREATE,
    EXISTS,
    GET_CHILDREN,
    GET_DATA,
    SET_DATA,
    DELETE
  }

  /**
   * Returns the number of milliseconds to wait before retrying the operation.
   *
   * @param failureCount Number of times that the request has been failed.
   * @param startTime Timestamp in milliseconds that the request starts.
   * @param type Type of operation tried to perform.
   * @param path The path that the operation is acting on.
   * @return Number of milliseconds to wait before retrying the operation. Returning {@code 0} means
   *         retry it immediately, while negative means abort the operation.
   */
  long nextRetry(int failureCount, long startTime, OperationType type, String path);
}
