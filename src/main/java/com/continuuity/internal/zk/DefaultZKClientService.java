package com.continuuity.internal.zk;

import com.continuuity.weave.internal.utils.Threads;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.ZKClientService;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base implementation of {@link com.continuuity.zk.ZKClientService}.
 */
public final class DefaultZKClientService implements ZKClientService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultZKClientService.class);

  private final String zkStr;
  private final int sessionTimeout;
  private final Watcher connectionWatcher;
  private final AtomicReference<ZooKeeper> zooKeeper;
  private final Function<String, List<ACL>> aclMapper;
  private final Service serviceDelegate;
  private ExecutorService eventExecutor;

  public DefaultZKClientService(String zkStr, int sessionTimeout, Watcher connectionWatcher) {
    this.zkStr = zkStr;
    this.sessionTimeout = sessionTimeout;
    this.connectionWatcher = wrapWatcher(connectionWatcher);
    this.zooKeeper = new AtomicReference<ZooKeeper>();

    // TODO: Add ACL
    aclMapper = new Function<String, List<ACL>>() {
      @Override
      public List<ACL> apply(String input) {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
      }
    };
    serviceDelegate = new ServiceDelegate();
  }

  @Override
  public OperationFuture<String> create(String path, byte[] data, CreateMode createMode) {
    return create(path, data, createMode, true);
  }

  @Override
  public OperationFuture<String> create(final String path,
                                        @Nullable final byte[] data,
                                        final CreateMode createMode,
                                        final boolean createParent) {
    return doCreate(path, data, createMode, createParent, false);
  }

  private OperationFuture<String> doCreate(final String path,
                                        @Nullable final byte[] data,
                                        final CreateMode createMode,
                                        final boolean createParent,
                                        final boolean ignoreNodeExists) {
    final SettableOperationFuture<String> createFuture = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().create(path, data, aclMapper.apply(path), createMode, Callbacks.STRING, createFuture);
    if (!createParent) {
      return createFuture;
    }

    // If create parent is request, return a different future
    final SettableOperationFuture<String> result = SettableOperationFuture.create(path, eventExecutor);
    // Watch for changes in the original future
    Futures.addCallback(createFuture, new FutureCallback<String>() {
      @Override
      public void onSuccess(String path) {
        // Propagate if creation was successful
        result.set(path);
      }

      @Override
      public void onFailure(Throwable t) {
        // Propagate if there is error
        if (!(t instanceof KeeperException)) {
          result.setException(t);
          return;
        }
        KeeperException.Code code = ((KeeperException) t).code();

        // Node already exists, simply return success
        if (ignoreNodeExists && code == KeeperException.Code.NODEEXISTS) {
          // The requested path could be used because it only applies to non-sequential node
          result.set(path);
          return;
        }

        if (code != KeeperException.Code.NONODE) {
          result.setException(t);
          return;
        }

        // Create the parent node
        String parentPath = path.substring(0, path.lastIndexOf('/'));
        if (parentPath.isEmpty()) {
          result.setException(new IllegalStateException("Root node not exists."));
          return;
        }
        // Watch for parent creation complete
        Futures.addCallback(
          doCreate(parentPath, null, CreateMode.PERSISTENT, createParent, true), new FutureCallback<String>() {
          @Override
          public void onSuccess(String parentPath) {
            // Create the requested path again
            getZooKeeper().create(path, data, aclMapper.apply(path), createMode, Callbacks.STRING, result);
          }

          @Override
          public void onFailure(Throwable t) {
            result.setException(t);
          }
        });
      }
    });

    return result;
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return exists(path, null);
  }

  @Override
  public OperationFuture<Stat> exists(String path, Watcher watcher) {
    SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().exists(path, wrapWatcher(watcher), Callbacks.STAT, result);
    return result;
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return getChildren(path, null);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, Watcher watcher) {
    SettableOperationFuture<NodeChildren> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().getChildren(path, wrapWatcher(watcher), Callbacks.CHILDREN, result);
    return result;
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return getData(path, null);
  }

  @Override
  public OperationFuture<NodeData> getData(String path, Watcher watcher) {
    SettableOperationFuture<NodeData> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().getData(path, wrapWatcher(watcher), Callbacks.DATA, result);

    return result;
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return setData(path, data, -1);
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    SettableOperationFuture<Stat> result = SettableOperationFuture.create(dataPath, eventExecutor);
    getZooKeeper().setData(dataPath, data, version, Callbacks.STAT, result);
    return result;
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return delete(path, -1);
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    SettableOperationFuture<String> result = SettableOperationFuture.create(deletePath, eventExecutor);
    getZooKeeper().delete(deletePath, version, Callbacks.VOID, result);
    return result;
  }

  @Override
  public Supplier<ZooKeeper> getZooKeeperSupplier() {
    return new Supplier<ZooKeeper>() {
      @Override
      public ZooKeeper get() {
        return getZooKeeper();
      }
    };
  }

  @Override
  public ListenableFuture<State> start() {
    return serviceDelegate.start();
  }

  @Override
  public State startAndWait() {
    return serviceDelegate.startAndWait();
  }

  @Override
  public boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public State state() {
    return serviceDelegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  @Override
  public State stopAndWait() {
    return serviceDelegate.stopAndWait();
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    serviceDelegate.addListener(listener, executor);
  }

  private ZooKeeper getZooKeeper() {
    ZooKeeper zk = zooKeeper.get();
    Preconditions.checkArgument(zk != null, "Not connected to zooKeeper.");
    return zk;
  }

  private Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null) {
      return null;
    }
    return new Watcher() {
      @Override
      public void process(final WatchedEvent event) {
        eventExecutor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              watcher.process(event);
            } catch (Throwable t) {
              LOG.error("Watcher throws exception.", t);
            }
          }
        });
      }
    };
  }

  private final class ServiceDelegate extends AbstractService implements Watcher {

    @Override
    protected void doStart() {
      // A single thread executor
      eventExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                                             Threads.createDaemonThreadFactory("zk-client-EventThread")) {
        @Override
        protected void terminated() {
          super.terminated();
          notifyStopped();
        }
      };

      try {
        zooKeeper.set(new ZooKeeper(zkStr, sessionTimeout, this));
      } catch (IOException e) {
        notifyFailed(e);
      }
    }

    @Override
    protected void doStop() {
      ZooKeeper zk = zooKeeper.getAndSet(null);
      if (zk != null) {
        try {
          zk.close();
        } catch (InterruptedException e) {
          notifyFailed(e);
        } finally {
          eventExecutor.shutdown();
        }
      }
    }

    @Override
    public void process(WatchedEvent event) {
      try {
        if (event.getState() == Event.KeeperState.SyncConnected && state() == State.STARTING) {
          LOG.info("Connected to ZooKeeper: " + zkStr);
          notifyStarted();
          return;
        }
        if (event.getState() == Event.KeeperState.Expired) {
          LOG.info("ZooKeeper session expired: " + zkStr);

          // When connection expired, simple reconnect again
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                zooKeeper.set(new ZooKeeper(zkStr, sessionTimeout, ServiceDelegate.this));
              } catch (IOException e) {
                zooKeeper.set(null);
                notifyFailed(e);
              }
            }
          }, "zk-reconnect");
          t.setDaemon(true);
          t.start();
        }
      } finally {
        if (connectionWatcher != null && event.getType() == Event.EventType.None) {
          connectionWatcher.process(event);
        }
      }
    }
  }

  /**
   * Collection of generic callbacks that simply reflect results into OperationFuture.
   */
  private static final class Callbacks {
    static AsyncCallback.StringCallback STRING = new AsyncCallback.StringCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, String name) {
        SettableOperationFuture<String> result = (SettableOperationFuture<String>)ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(name);
          return;
        }
        result.setException(KeeperException.create(code));
      }
    };

    static AsyncCallback.StatCallback STAT = new AsyncCallback.StatCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        SettableOperationFuture<Stat> result = (SettableOperationFuture<Stat>)ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK || code == KeeperException.Code.NONODE) {
          result.set(stat);
          return;
        }
        result.setException(KeeperException.create(code));
      }
    };

    static AsyncCallback.Children2Callback CHILDREN = new AsyncCallback.Children2Callback() {
      @Override
      public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        SettableOperationFuture<NodeChildren> result = (SettableOperationFuture<NodeChildren>)ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(new BasicNodeChildren(children, stat));
          return;
        }
        result.setException(KeeperException.create(code));
      }
    };

    static AsyncCallback.DataCallback DATA = new AsyncCallback.DataCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        SettableOperationFuture<NodeData> result = (SettableOperationFuture<NodeData>)ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(new BasicNodeData(data, stat));
          return;
        }
        result.setException(KeeperException.create(code));
      }
    };

    static AsyncCallback.VoidCallback VOID = new AsyncCallback.VoidCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx) {
        SettableOperationFuture<String> result = (SettableOperationFuture<String>)ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(result.getRequestPath());
          return;
        }
        // Otherwise, it is an error
        result.setException(KeeperException.create(code));
      }
    };
  }
}
