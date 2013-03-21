package com.continuuity.weave.internal.zk;

import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 */
public interface OperationFuture<V> extends ListenableFuture<V> {

  /**
   * @return The path being requested for the ZooKeeper operation.
   */
  String getRequestPath();
}
