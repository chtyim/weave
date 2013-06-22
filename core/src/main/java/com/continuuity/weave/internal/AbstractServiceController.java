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
import com.continuuity.weave.internal.json.StackTraceElementCodec;
import com.continuuity.weave.internal.json.StateNodeCodec;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.GsonBuilder;
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
 * Abstract base class for controlling remote service through ZooKeeper.
 */
public abstract class AbstractServiceController implements ServiceController, Cancellable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceController.class);

  private final RunId runId;
  private final AtomicReference<StateNode> stateNode;
  private final ListenerExecutors listenerExecutors;
  private final AtomicReference<byte[]> liveNodeData;
  private final FutureCallback<NodeData> nodeDataFutureCallback;
  private volatile Cancellable watchCanceller;
  protected final ZKClient zkClient;

  protected AbstractServiceController(ZKClient zkClient, RunId runId) {
    this.zkClient = zkClient;
    this.runId = runId;
    this.stateNode = new AtomicReference<StateNode>(new StateNode(State.UNKNOWN, null));
    this.listenerExecutors = new ListenerExecutors();
    this.liveNodeData = new AtomicReference<byte[]>();
    this.nodeDataFutureCallback = createNodeDataCallback();
  }

  public void start() {
    // Watch for state changes
    final Cancellable cancelState = ZKOperations.watchData(zkClient, getZKPath("state"),
                                                           new ZKOperations.DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        fireStateChange(decode(nodeData));
      }
    });

    final Cancellable cancelInstance = watchInstanceNode();

    watchCanceller = new Cancellable() {
      @Override
      public void cancel() {
        cancelState.cancel();
        cancelInstance.cancel();
      }
    };
  }

  /**
   * Caution. This method is for internal use only to release resources of this controller without stopping
   * the running service. After this method is called, it is expected to discard that instance.
   */
  @Override
  public void cancel() {
    if (watchCanceller != null) {
      watchCanceller.cancel();
    }
  }

  protected byte[] getLiveNodeData() {
    return liveNodeData.get();
  }

  protected <V> ListenableFuture<V> sendMessage(Message message, V result) {
    return ZKMessages.sendMessage(zkClient, getMessagePrefix(), message, result);
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public ListenableFuture<Command> sendCommand(Command command) {
    return ZKMessages.sendMessage(zkClient, getMessagePrefix(), Messages.createForAll(command), command);
  }

  @Override
  public ListenableFuture<Command> sendCommand(String runnableName, Command command) {
    return ZKMessages.sendMessage(zkClient, getMessagePrefix(),
                                  Messages.createForRunnable(runnableName, command), command);
  }

  @Override
  public State getState() {
    return stateNode.get().getState();
  }

  @Override
  public ListenableFuture<State> stop() {
    State oldState = stateNode.get().getState();
    if (oldState == State.UNKNOWN || oldState == State.STARTING || oldState == State.RUNNING) {
      watchCanceller.cancel();
      return Futures.transform(
        ZKMessages.sendMessage(zkClient, getMessagePrefix(), SystemMessages.stopApplication(),
                               State.TERMINATED), new AsyncFunction<State, State>() {
        @Override
        public ListenableFuture<State> apply(State input) throws Exception {
          // Wait for the instance ephemeral node goes away
          return Futures.transform(
            ZKOperations.watchDeleted(zkClient, getInstancePath()), new Function<String, State>() {
               @Override
               public State apply(String input) {
                 LOG.info("Remote service stopped: " + runId.getId());
                 fireStateChange(new StateNode(State.TERMINATED, null));
                 return State.TERMINATED;
               }
             });
        }
      });
    }
    return Futures.immediateFuture(getState());
  }

  @Override
  public void stopAndWait() {
    Futures.getUnchecked(stop());
  }

  @Override
  public Cancellable addListener(Listener listener, Executor executor) {
    ListenerExecutor listenerExecutor = new ListenerExecutor(listener, executor);
    Cancellable cancellable = listenerExecutors.addListener(listenerExecutor);
    dispatchListener(stateNode.get(), listenerExecutor);
    return cancellable;
  }

  protected final String getMessagePrefix() {
    return getZKPath("messages/msg");
  }

  /**
   * Returns the zookeeper node path for the ephemeral instance node for this runId.
   */
  protected String getInstancePath() {
    return String.format("/instances/%s", runId.getId());
  }

  protected void instanceNodeRemoved() {
    fireStateChange(new StateNode(State.TERMINATED, new StackTraceElement[]));
  }

  protected final void fireStateChange(StateNode state) {
    stateNode.set(state);
    switch (state.getState()) {
      case TERMINATED:
      case FAILED:
        watchCanceller.cancel();
    }
    dispatchListener(state, listenerExecutors);
  }

  private StateNode decode(NodeData nodeData) {
    // Node data and data inside shouldn't be null. If it does, the service must not be running anymore.
    if (nodeData == null) {
      return new StateNode(State.TERMINATED, null);
    }
    byte[] data = nodeData.getData();
    if (data == null) {
      return new StateNode(State.TERMINATED, null);
    }
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNodeCodec())
      .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
      .create()
      .fromJson(new String(data, Charsets.UTF_8), StateNode.class);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", runId.getId(), path);
  }

  private void dispatchListener(StateNode state, Listener listener) {
    switch (state.getState()) {
      case STARTING:
        listener.starting();
        break;
      case RUNNING:
        listener.running();
        break;
      case STOPPING:
        listener.stopping();
        break;
      case TERMINATED:
        listener.terminated();
        break;
      case FAILED:
        listener.failed(state.getStackTraces());
        break;
    }
  }

  /**
   * Repeatedly watch for changes in the run instance ephemeral node.
   * @return A {@link Cancellable} that will cancel the watch action.
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
        if (result != null) {
          Futures.addCallback(zkClient.getData(getInstancePath()), nodeDataFutureCallback);
        }```````````
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

  private FutureCallback<NodeData> createNodeDataCallback() {
    return new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        liveNodeData.set(result.getData());
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch node data.", t);
        liveNodeData.set(null);
      }
    };
  }

  private static final class ListenerExecutors implements Listener {
    private final Queue<ListenerExecutor> listeners = new ConcurrentLinkedQueue<ListenerExecutor>();

    Cancellable addListener(final ListenerExecutor listener) {
      listeners.add(listener);
      return new Cancellable() {
        @Override
        public void cancel() {
          listeners.remove(listener);
        }
      };
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
    public void stopping() {
      for (ListenerExecutor listener : listeners) {
        listener.stopping();
      }
    }

    @Override
    public void terminated() {
      for (ListenerExecutor listener : listeners) {
        listener.terminated();
      }
    }

    @Override
    public void failed(StackTraceElement[] stackTraces) {
      for (ListenerExecutor listener : listeners) {
        listener.failed(stackTraces);
      }
    }
  }

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
    public void stopping() {
      if (hasCalled(State.STOPPING)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.stopping();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void terminated() {
      if (hasCalled(State.TERMINATED) || hasCalled(State.FAILED)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.terminated();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void failed(final StackTraceElement[] stackTraces) {
      if (hasCalled(State.FAILED) || hasCalled(State.TERMINATED)) {
        return;
      }
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.failed(stackTraces);
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
