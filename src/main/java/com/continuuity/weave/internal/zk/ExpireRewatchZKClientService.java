package com.continuuity.weave.internal.zk;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
*
*/
final class ExpireRewatchZKClientService extends ForwardingZKClientService {

  ExpireRewatchZKClientService(ZKClientService delegate) {
    super(delegate);
  }

  @Override
  public OperationFuture<Stat> exists(String path, Watcher watcher) {
    final ExpireRewatchWatcher wrappedWatcher = new ExpireRewatchWatcher(this, ExpireRewatchWatcher.ActionType.EXISTS,
                                                                       path, watcher);
    OperationFuture<Stat> result = super.exists(path, wrappedWatcher);
    Futures.addCallback(result, new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        wrappedWatcher.setLastResult(result);
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op
      }
    });
    return result;
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, Watcher watcher) {
    final ExpireRewatchWatcher wrappedWatcher = new ExpireRewatchWatcher(this, ExpireRewatchWatcher.ActionType.CHILDREN,
                                                                       path, watcher);
    OperationFuture<NodeChildren> result = super.getChildren(path, wrappedWatcher);
    Futures.addCallback(result, new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        wrappedWatcher.setLastResult(result);
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op
      }
    });
    return result;
  }

  @Override
  public OperationFuture<NodeData> getData(String path, Watcher watcher) {
    final ExpireRewatchWatcher wrappedWatcher = new ExpireRewatchWatcher(this, ExpireRewatchWatcher.ActionType.DATA,
                                                                       path, watcher);
    OperationFuture<NodeData> result = super.getData(path, wrappedWatcher);
    Futures.addCallback(result, new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        wrappedWatcher.setLastResult(result);
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op
      }
    });
    return result;

  }
}
