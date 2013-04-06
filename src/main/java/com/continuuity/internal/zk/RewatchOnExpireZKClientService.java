package com.continuuity.internal.zk;

import com.continuuity.internal.zk.RewatchOnExpireWatcher.ActionType;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.ZKClientService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * A {@link com.continuuity.zk.ZKClientService} that will rewatch automatically when session expired and reconnect.
 * The rewatch logic is mainly done in {@link RewatchOnExpireWatcher}.
 */
public final class RewatchOnExpireZKClientService extends ForwardingZKClientService {

  public RewatchOnExpireZKClientService(ZKClientService delegate) {
    super(delegate);
  }

  @Override
  public OperationFuture<Stat> exists(String path, Watcher watcher) {
    final RewatchOnExpireWatcher wrappedWatcher = new RewatchOnExpireWatcher(this, ActionType.EXISTS, path, watcher);
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
    final RewatchOnExpireWatcher wrappedWatcher = new RewatchOnExpireWatcher(this, ActionType.CHILDREN, path, watcher);
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
    final RewatchOnExpireWatcher wrappedWatcher = new RewatchOnExpireWatcher(this, ActionType.DATA, path, watcher);
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
