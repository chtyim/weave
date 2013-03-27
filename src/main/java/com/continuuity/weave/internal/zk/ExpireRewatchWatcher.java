package com.continuuity.weave.internal.zk;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * A wrapper for {@link Watcher} that will re-set the watch automatically until it is successful.
 */
final class ExpireRewatchWatcher implements Watcher {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireRewatchWatcher.class);

  enum ActionType {
    EXISTS,
    CHILDREN,
    DATA
  }

  private final ZKClientService client;
  private final ActionType actionType;
  private final String path;
  private final Watcher delegate;
  private final AtomicMarkableReference<Object> lastResult;

  ExpireRewatchWatcher(ZKClientService client, ActionType actionType, String path, Watcher delegate) {
    this.client = client;
    this.actionType = actionType;
    this.path = path;
    this.delegate = delegate;
    this.lastResult = new AtomicMarkableReference<Object>(null, false);
  }

  void setLastResult(Object result) {
    lastResult.compareAndSet(null, result, false, true);
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() != Event.EventType.None) {
      delegate.process(event);
    }

    if (event.getState() != Event.KeeperState.Expired) {
      return;
    }
    switch (actionType) {
      case EXISTS:
        exists();
        break;
      case CHILDREN:
        children();
        break;
      case DATA:
        data();
        break;
    }
  }

  private void exists() {
    Futures.addCallback(client.exists(path, this), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat stat) {
        // Since we know all callbacks and watcher are triggered from single event thread, there is no race condition.
        Object oldResult = lastResult.getReference();
        lastResult.compareAndSet(oldResult, null, true, false);

        if (stat != oldResult && (stat == null || !stat.equals(oldResult))) {
          if (stat == null) {
            // previous stat is not null, means node deleted
            process(new WatchedEvent(Event.EventType.NodeDeleted, Event.KeeperState.SyncConnected, path));
          } else if (oldResult == null) {
            // previous stat is null, means node created
            process(new WatchedEvent(Event.EventType.NodeCreated, Event.KeeperState.SyncConnected, path));
          } else {
            // Otherwise, something changed on the node
            process(new WatchedEvent(Event.EventType.NodeDataChanged, Event.KeeperState.SyncConnected, path));
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (RetryUtils.canRetry(t)) {
          exists();
        } else {
          lastResult.set(null, false);
          LOG.error("Fail to re-set watch on exists for path " + path, t);
        }
      }
    });
  }

  private void children() {
    Futures.addCallback(client.getChildren(path, this), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        Object oldResult = lastResult.getReference();
        lastResult.compareAndSet(oldResult, null, true, false);

        if (result.equals(oldResult)) {
          return;
        }

        if (!(oldResult instanceof NodeChildren)) {
          // Something very wrong
          LOG.error("The same watcher has been used for different event type.");
          return;
        }

        NodeChildren oldNodeChildren = (NodeChildren)oldResult;
        if (!result.getChildren().equals(oldNodeChildren.getChildren())) {
          process(new WatchedEvent(Event.EventType.NodeChildrenChanged, Event.KeeperState.SyncConnected, path));
        } else {
          process(new WatchedEvent(Event.EventType.NodeDataChanged, Event.KeeperState.SyncConnected, path));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (RetryUtils.canRetry(t)) {
          children();
          return;
        }

        lastResult.set(null, false);
        if (t instanceof KeeperException) {
          KeeperException.Code code = ((KeeperException) t).code();
          if (code == KeeperException.Code.NONODE) {
            // Node deleted
            process(new WatchedEvent(Event.EventType.NodeDeleted, Event.KeeperState.SyncConnected, path));
            return;
          }
        }
        LOG.error("Fail to re-set watch on getChildren for path " + path, t);
      }
    });
  }

  private void data() {
    Futures.addCallback(client.getData(path, this), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        Object oldResult = lastResult.getReference();
        lastResult.compareAndSet(oldResult, null, true, false);

        if (!result.equals(oldResult)) {
          // Whenever something changed, treated it as data changed.
          process(new WatchedEvent(Event.EventType.NodeDataChanged, Event.KeeperState.SyncConnected, path));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (RetryUtils.canRetry(t)) {
          data();
          return;
        }

        lastResult.set(null, false);
        if (t instanceof KeeperException) {
          KeeperException.Code code = ((KeeperException) t).code();
          if (code == KeeperException.Code.NONODE) {
            // Node deleted
            process(new WatchedEvent(Event.EventType.NodeDeleted, Event.KeeperState.SyncConnected, path));
            return;
          }
        }
        LOG.error("Fail to re-set watch on getData for path " + path, t);
      }
    });
  }
}
