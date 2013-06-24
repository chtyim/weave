/**
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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.json.StackTraceElementCodec;
import com.continuuity.weave.internal.json.StateNodeCodec;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.MessageCodec;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Service} decorator that wrap another {@link Service} with the service states reflected
 * to ZooKeeper.
 */
public final class ZKServiceDecorator extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ZKServiceDecorator.class);

  private final ZKClient zkClient;
  private final RunId id;
  private final Supplier<? extends JsonElement> liveNodeData;
  private final Service decoratedService;
  private final MessageCallbackCaller messageCallback;
  private ExecutorService callbackExecutor;

  public ZKServiceDecorator(ZKClient zkClient, RunId id,
                            Supplier<? extends JsonElement> liveNodeData, Service decoratedService) {
    this.zkClient = zkClient;
    this.id = id;
    this.liveNodeData = liveNodeData;
    this.decoratedService = decoratedService;
    if (decoratedService instanceof MessageCallback) {
      this.messageCallback = new MessageCallbackCaller((MessageCallback) decoratedService, zkClient);
    } else {
      this.messageCallback = new MessageCallbackCaller(zkClient);
    }
  }

  @Override
  protected void doStart() {
    callbackExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("message-callback"));
    zkClient.addConnectionWatcher(createConnectionWatcher());

    // Create nodes for states and messaging
    StateNode stateNode = new StateNode(ServiceController.State.STARTING, null);

    final List<OperationFuture<String>> futures = ImmutableList.of(
      zkClient.create(getZKPath("messages"), null, CreateMode.PERSISTENT),
      zkClient.create(getZKPath("state"), encodeStateNode(stateNode), CreateMode.PERSISTENT)
    );
    final ListenableFuture<List<String>> createFuture = Futures.successfulAsList(futures);

    createFuture.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          for (OperationFuture<String> future : futures) {
            try {
              future.get();
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof KeeperException
                && ((KeeperException) cause).code() == KeeperException.Code.NODEEXISTS) {
                LOG.warn("Node already exists: {}", future.getRequestPath());
              } else {
                throw Throwables.propagate(cause);
              }
            }
          }

          // Starts the decorated service
          decoratedService.addListener(createListener(), Threads.SAME_THREAD_EXECUTOR);
          decoratedService.start();
        } catch (Exception e) {
          notifyFailed(e);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  protected void doStop() {
    // Stops the decorated service
    decoratedService.stop();
    callbackExecutor.shutdownNow();
  }

  private Watcher createConnectionWatcher() {
    final AtomicReference<Watcher.Event.KeeperState> keeperState = new AtomicReference<Watcher.Event.KeeperState>();

    return new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // When connected (either first time or reconnected from expiration), creates a ephemeral node
        Event.KeeperState current = event.getState();
        Event.KeeperState previous = keeperState.getAndSet(current);

        LOG.info("Connection state changed " + previous + " => " + current);

        if (current == Event.KeeperState.SyncConnected && (previous == null || previous == Event.KeeperState.Expired)) {
          String liveNode = "/instances/" + id;
          LOG.info("Create live node " + liveNode);

          JsonObject content = new JsonObject();
          content.add("data", liveNodeData.get());
          listenFailure(zkClient.create(liveNode, encodeJson(content), CreateMode.EPHEMERAL));
        }
      }
    };
  }

  private void watchMessages() {
    final String messagesPath = getZKPath("messages");
    Futures.addCallback(zkClient.getChildren(messagesPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // TODO: Do we need to deal with other type of events?
        if (event.getType() == Event.EventType.NodeChildrenChanged && decoratedService.isRunning()) {
          watchMessages();
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        // Sort by the name, which is the messageId. Assumption is that message ids is ordered by time.
        List<String> messages = Lists.newArrayList(result.getChildren());
        Collections.sort(messages);
        for (String messageId : messages) {
          processMessage(messagesPath + "/" + messageId, messageId);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // TODO: what could be done besides just logging?
        LOG.error("Failed to watch messages.", t);
      }
    });
  }

  private void processMessage(final String path, final String messageId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        Message message = MessageCodec.decode(result.getData());
        if (message == null) {
          LOG.error("Failed to decode message for " + messageId + " in " + path);
          listenFailure(zkClient.delete(path, result.getStat().getVersion()));
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Message received from " + path + ": " + new String(MessageCodec.encode(message), Charsets.UTF_8));
        }
        if (handleStopMessage(message, getDeleteSupplier(path, result.getStat().getVersion()))) {
          return;
        }
        messageCallback.onReceived(callbackExecutor, path, result.getStat().getVersion(), messageId, message);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch message content.", t);
      }
    });
  }

  private <V> boolean handleStopMessage(Message message, final Supplier<OperationFuture<V>> postHandleSupplier) {
    if (message.getType() == Message.Type.SYSTEM && "stop".equalsIgnoreCase(message.getCommand().getCommand())) {
      callbackExecutor.execute(new Runnable() {
        @Override
        public void run() {
          decoratedService.stop().addListener(new Runnable() {

            @Override
            public void run() {
              stopServiceOnComplete(postHandleSupplier.get(), ZKServiceDecorator.this);
            }
          }, MoreExecutors.sameThreadExecutor());
        }
      });
      return true;
    }
    return false;
  }


  private Supplier<OperationFuture<String>> getDeleteSupplier(final String path, final int version) {
    return new Supplier<OperationFuture<String>>() {
      @Override
      public OperationFuture<String> get() {
        return zkClient.delete(path, version);
      }
    };
  }

  private Listener createListener() {
    return new DecoratedServiceListener();
  }

  private <V> byte[] encode(V data, Class<? extends V> clz) {
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNodeCodec())
                            .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
                            .create()
      .toJson(data, clz).getBytes(Charsets.UTF_8);
  }

  private byte[] encodeStateNode(StateNode stateNode) {
    return encode(stateNode, StateNode.class);
  }

  private <V extends JsonElement> byte[] encodeJson(V json) {
    return new Gson().toJson(json).getBytes(Charsets.UTF_8);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", id, path);
  }

  private static <V> OperationFuture<V> listenFailure(final OperationFuture<V> operationFuture) {
    operationFuture.addListener(new Runnable() {

      @Override
      public void run() {
        try {
          if (!operationFuture.isCancelled()) {
            operationFuture.get();
          }
        } catch (Exception e) {
          // TODO: what could be done besides just logging?
          LOG.error("Operation execution failed for " + operationFuture.getRequestPath(), e);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return operationFuture;
  }

  private static final class MessageCallbackCaller {
    private final MessageCallback callback;
    private final ZKClient zkClient;

    private MessageCallbackCaller(ZKClient zkClient) {
      this(null, zkClient);
    }

    private MessageCallbackCaller(MessageCallback callback, ZKClient zkClient) {
      this.callback = callback;
      this.zkClient = zkClient;
    }

    public void onReceived(Executor executor, final String path,
                           final int version, final String id, final Message message) {
      if (callback == null) {
        // Simply delete the message
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring incoming message from " + path + ": " + message);
        }
        listenFailure(zkClient.delete(path, version));
        return;
      }

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // Message process is synchronous for now. Making it async needs more thoughts about race conditions.
            // The executor is the callbackExecutor which is a single thread executor.
            callback.onReceived(id, message).get();
          } catch (Throwable t) {
            LOG.error("Exception when processing message: {}, {}, {}", id, message, path, t);
          } finally {
            listenFailure(zkClient.delete(path, version));
          }
        }
      });
    }
  }

  private final class DecoratedServiceListener implements Listener {
    private volatile boolean zkFailure = false;

    @Override
    public void starting() {
      LOG.info("Starting: " + id);
      saveState(ServiceController.State.STARTING);
    }

    @Override
    public void running() {
      LOG.info("Running: " + id);
      notifyStarted();
      watchMessages();
      saveState(ServiceController.State.RUNNING);
    }

    @Override
    public void stopping(State from) {
      LOG.info("Stopping: " + id);
      saveState(ServiceController.State.STOPPING);
    }

    @Override
    public void terminated(State from) {
      LOG.info("Terminated: " + from + " " + id);
      if (zkFailure) {
        return;
      }
      StateNode stateNode = new StateNode(ServiceController.State.TERMINATED, null);
      Futures.addCallback(zkClient.setData(getZKPath("state"), encodeStateNode(stateNode)), new FutureCallback<Stat>() {
          @Override
          public void onSuccess(Stat result) {
            notifyStopped();
          }

          @Override
          public void onFailure(Throwable t) {
            notifyFailed(t);
          }
        });
    }

    @Override
    public void failed(State from, final Throwable failure) {
      LOG.info("Failed: " + from + " " + id + ". Reason: " + failure, failure);
      if (zkFailure) {
        return;
      }

      StateNode stateNode = new StateNode(ServiceController.State.FAILED, failure.getStackTrace());
      zkClient.setData(getZKPath("state"), encodeStateNode(stateNode)).addListener(new Runnable() {
        @Override
        public void run() {
          notifyFailed(failure);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }

    private void saveState(ServiceController.State state) {
      if (zkFailure) {
        return;
      }
      StateNode stateNode = new StateNode(state, null);
      stopOnFailure(zkClient.setData(getZKPath("state"), encodeStateNode(stateNode)));
    }

    private <V> void stopOnFailure(final OperationFuture<V> future) {
      future.addListener(new Runnable() {
        @Override
        public void run() {
          try {
            future.get();
          } catch (final Exception e) {
            LOG.error("ZK operation failed", e);
            zkFailure = true;
            decoratedService.stop().addListener(new Runnable() {
              @Override
              public void run() {
                notifyFailed(e);
              }
            }, Threads.SAME_THREAD_EXECUTOR);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }
  }

  private <V> ListenableFuture<State> stopServiceOnComplete(ListenableFuture <V> future, final Service service) {
    return Futures.transform(future, new AsyncFunction<V, State>() {
      @Override
      public ListenableFuture<State> apply(V input) throws Exception {
        return service.stop();
      }
    });
  }
}
