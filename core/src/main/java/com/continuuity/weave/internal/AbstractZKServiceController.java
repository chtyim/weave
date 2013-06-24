/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.internal;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract base class for implementing a {@link ServiceController} using ZooKeeper as a means for
 * communicating with the remote service. This is designed to work in pair with the {@link ZKServiceDecorator}.
 */
public abstract class AbstractZKServiceController implements ServiceController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractZKServiceController.class);

  private final ZKClient zkClient;
  private final RunId runId;
  private final ServiceDelegate serviceDelegate;
  private final LiveNodeDataSupplier liveNodeDataSupplier;

  protected AbstractZKServiceController(ZKClient zkClient, RunId runId) {
    this.zkClient = zkClient;
    this.runId = runId;
    this.serviceDelegate = new ServiceDelegate();
    this.liveNodeDataSupplier = new LiveNodeDataSupplier();
  }

  @Override
  public final RunId getRunId() {
    return runId;
  }

  @Override
  public ListenableFuture<Command> sendCommand(Command command) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ListenableFuture<Command> sendCommand(String runnableName, Command command) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Repeatedly watch for changes in the running instance ephemeral node.
   * @return A {@link com.continuuity.weave.common.Cancellable} that will cancel the watch action.
   */
  private Cancellable watchInstanceNode() {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    Futures.addCallback(zkClient.exists(getInstancePath(), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (cancelled.get()) {
          return;
        }
        switch (event.getType()) {
          case NodeCreated:
          case NodeDataChanged:
            watchInstanceNode();
            break;
          case NodeDeleted:
            instanceNodeRemoved();
            break;
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {     // Node exists
          Futures.addCallback(zkClient.getData(getInstancePath()), liveNodeDataSupplier);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to check exists on instance node {}", getInstancePath());
      }
    });
    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  /**
   * Called when the instance/RunId ZK node is removed. It signals the ephemeral node process owner is
   * gone or is no longer visible due to ZK timeout. Children classes needs to implement this method to
   * act on such event and potentially changing the state of this Service to stopped.
   */
  protected abstract void instanceNodeRemoved();

  /**
   * Returns the zookeeper node path for the ephemeral instance node for this runId.
   */
  protected final String getInstancePath() {
    return String.format("/instances/%s", runId.getId());
  }

  /**
   * Returns the latest NodeData for the instance/RunId node.
   */
  protected final NodeData getLiveNodeData() {
    return liveNodeDataSupplier.get();
  }

  @Override
  public final ListenableFuture<State> start() {
    return serviceDelegate.start();
  }

  @Override
  public final State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public final boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public final State state() {
    return serviceDelegate.state();
  }

  @Override
  public final ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  @Override
  public final State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public final void addListener(Listener listener, Executor executor) {

  }

  /**
   * Internal helper class for providing latest NodeData of the instance/runId ephemeral node.
   */
  protected final class LiveNodeDataSupplier implements Supplier<NodeData>, FutureCallback<NodeData> {

    private final AtomicReference<NodeData> nodeData = new AtomicReference<NodeData>();

    @Override
    public void onSuccess(NodeData result) {
      nodeData.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Failed in fetching live node data.", t);
      nodeData.set(null);
    }

    @Override
    public NodeData get() {
      return nodeData.get();
    }
  }

  /**
   * Internal class for maintaining the Service lifecycle.
   */
  private final class ServiceDelegate extends AbstractService {

    @Override
    protected void doStart() {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doStop() {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    public void doNotifyStarted() {
      notifyStarted();
    }

    public void doNotifyStopped() {
      notifyStopped();
    }

    public void doNotifyFailed(Throwable cause) {
      notifyFailed(cause);
    }
  }

  /**
   * Inner class for dispatching listener call back to a list of listeners
   */
  private static final class ListenerExecutors implements Listener {
    private final Queue<ListenerExecutor> listeners = new ConcurrentLinkedQueue<ListenerExecutor>();

    void addListener(final ListenerExecutor listener) {
      listeners.add(listener);
    }

    @Override
    public void starting() {
      for (ListenerExecutor listener : listeners) {
        listener.starting();
      }
    }

    @Override
    public void running() {
      for (ListenerExecutor listener : listeners) {
        listener.running();
      }
    }

    @Override
    public void stopping(State from) {
      for (ListenerExecutor listener : listeners) {
        listener.stopping(from);
      }
    }

    @Override
    public void terminated(State from) {
      for (ListenerExecutor listener : listeners) {
        listener.terminated(from);
      }
    }

    @Override
    public void failed(State from, Throwable failure) {
      for (ListenerExecutor listener : listeners) {
        listener.failed(from, failure);
      }
    }
  }

  /**
   * Wrapper for {@link Listener} to have callback executed on a given {@link Executor}.
   * Also make sure each method is called at most once.
   */
  private static final class ListenerExecutor implements Listener {

    private final Listener delegate;
    private final Executor executor;
    private final ConcurrentMap<State, Boolean> callStates = Maps.newConcurrentMap();

    private ListenerExecutor(Listener delegate, Executor executor) {
      this.delegate = delegate;
      this.executor = executor;
    }

    @Override
    public void starting() {
      if (hasCalled(State.STARTING)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.starting();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void running() {
      if (hasCalled(State.RUNNING)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.running();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void stopping(final State from) {
      if (hasCalled(State.STOPPING)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.stopping(from);
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void terminated(final State from) {
      if (hasCalled(State.TERMINATED)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.terminated(from);
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void failed(final State from, final Throwable failure) {
      // Both failed and terminate are using the same state for checking as only either one could be called.
      if (hasCalled(State.TERMINATED)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.failed(from, failure);
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    private boolean hasCalled(State state) {
      return callStates.putIfAbsent(state, true) != null;
    }
  }
}
