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
package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.utils.Threads;
import com.continuuity.zk.ForwardingZKClient;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.RetryStrategies;
import com.continuuity.zk.ZKClient;
import com.continuuity.zk.ZKClientService;
import com.continuuity.zk.ZKClientServices;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Service} decorator that wrap another {@link Service} with the service states reflected
 * to ZooKeeper.
 */
public final class ZKServiceDecorator extends AbstractService {

  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
  private static Logger LOG = LoggerFactory.getLogger(ZKServiceDecorator.class);

  private final ZKClientService zkClient;
  private final RunId id;
  private final Supplier<? extends JsonElement> liveNodeData;
  private final Service decoratedService;
  private final MessageCallbackCaller messageCallback;
  private ExecutorService callbackExecutor;

  public ZKServiceDecorator(String zkConnect, int zkTimeout, RunId id,
                            Supplier<? extends JsonElement> liveNodeData, Service decoratedService) {
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
                      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                                        .setSessionTimeout(zkTimeout)
                                                        .setConnectionWatcher(createConnectionWatcher())
                                                        .build(),
                                                      RetryStrategies.exponentialDelay(100, 2000,
                                                                                       TimeUnit.MILLISECONDS)));
    this.id = id;
    this.liveNodeData = liveNodeData;
    this.decoratedService = decoratedService;
    if (decoratedService instanceof MessageCallback) {
      this.messageCallback = new MessageCallbackCaller((MessageCallback) decoratedService, zkClient);
    } else {
      this.messageCallback = new MessageCallbackCaller();
    }
  }

  public ZKClient getZKClient() {
    return new ForwardingZKClient(zkClient) {
    };
  }

  @Override
  protected void doStart() {
    callbackExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("message-callback"));

    // Connect to zookeeper.
    Futures.addCallback(zkClient.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        // Create nodes for states and messaging
        JsonObject stateContent = new JsonObject();
        stateContent.addProperty("state", "IDLE");
        createMessagesNode();
        final OperationFuture<String> stateFuture = zkClient.create(getZKPath("state"), encode(stateContent),
                                                              CreateMode.PERSISTENT);
        stateFuture.addListener(new Runnable() {
          @Override
          public void run() {
            try {
              stateFuture.get();
              // Starts the decorated service
              decoratedService.addListener(createListener(), SAME_THREAD_EXECUTOR);
              decoratedService.start();
            } catch (Exception e) {
              notifyFailed(e);
            }
          }
        }, SAME_THREAD_EXECUTOR);
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    // Stops the decorated service
    decoratedService.stop();
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
          listenFailure(zkClient.create(liveNode, encode(content), CreateMode.EPHEMERAL));
        }
      }
    };
  }

  private void createMessagesNode() {
    final OperationFuture<String> future = zkClient.create(getZKPath("messages"), null, CreateMode.PERSISTENT);
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          future.get();
          watchMessages();
        } catch (Exception e) {
          // TODO: what could be done besides just logging?
          LOG.error("Failed to create node " + future.getRequestPath(), e);
        }
      }
    }, SAME_THREAD_EXECUTOR);
  }

  private void watchMessages() {
    final String messagesPath = getZKPath("messages");
    Futures.addCallback(zkClient.getChildren(messagesPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        watchMessages();
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
        byte[] data = result.getData();
        if (data == null) {
          LOG.error("Empty message content for " + messageId + " in " + path);
          return;
        }
        messageCallback.onReceived(callbackExecutor, path, messageId, result.getStat().getVersion(), data);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch message content.", t);
      }
    });
  }

  private Listener createListener() {
    return new DecoratedServiceListener();
  }

  private byte[] encode(JsonElement json) {
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
    }, SAME_THREAD_EXECUTOR);
    return operationFuture;
  }

  private static final class MessageCallbackCaller {
    private final MessageCallback callback;
    private final ZKClientService zkClient;

    private MessageCallbackCaller() {
      // No-op
      callback = null;
      zkClient = null;
    }

    private MessageCallbackCaller(MessageCallback callback, ZKClientService zkClient) {
      this.callback = callback;
      this.zkClient = zkClient;
    }

    public void onReceived(Executor executor, final String path,
                           final String id, final int version, final byte[] data) {
      if (callback == null) {
        return;
      }
      final Message message = Messages.decode(data);
      if (message == null) {
        LOG.warn("Failed to decode message for " + id + " in " + path + ". Message ignored.");
        listenFailure(zkClient.delete(path, version));
        return;
      }

      executor.execute(new Runnable() {

        @Override
        public void run() {
          Futures.addCallback(callback.onReceived(message), new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
              // Delete the message node when processing is completed successfully.
              listenFailure(zkClient.delete(path, version));
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to process message for " + id + " in " + path, t);
            }
          });
        }
      });
    }
  }

  private class DecoratedServiceListener implements Listener {
    private volatile boolean zkFailure = false;

    @Override
    public void starting() {
      LOG.info("Starting: " + id);
      saveState("STARTING");
    }

    @Override
    public void running() {
      LOG.info("Running: " + id);
      notifyStarted();
      saveState("RUNNING");
    }

    @Override
    public void stopping(State from) {
      LOG.info("Stopping: " + id);
      saveState("STOPPING");
    }

    @Override
    public void terminated(State from) {
      LOG.info("Terminated: " + from + " " + id);
      if (zkFailure) {
        return;
      }
      JsonObject stateContent = new JsonObject();
      stateContent.addProperty("state", "TERMINATED");
      Futures.addCallback(stopServiceOnComplete(zkClient.setData(getZKPath("state"), encode(stateContent)), zkClient),
        new FutureCallback<State>() {
          @Override
          public void onSuccess(State result) {
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
      LOG.info("Failed: " + from + " " + id);
      if (zkFailure) {
        return;
      }
      StringWriter stackTraceWriter = new StringWriter();
      failure.printStackTrace(new PrintWriter(stackTraceWriter));
      JsonObject stateContent = new JsonObject();
      stateContent.addProperty("state", "FAILED");
      stateContent.addProperty("stackTrace", stackTraceWriter.toString());
      stopServiceOnComplete(zkClient.setData(getZKPath("state"),
                                             encode(stateContent)), zkClient).addListener(new Runnable() {
        @Override
        public void run() {
          notifyFailed(failure);
        }
      }, SAME_THREAD_EXECUTOR);
    }

    private void saveState(String state) {
      if (zkFailure) {
        return;
      }
      JsonObject stateContent = new JsonObject();
      stateContent.addProperty("state", state);
      stopOnFailure(zkClient.setData(getZKPath("state"), encode(stateContent)));
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
            stopServiceOnComplete(decoratedService.stop(), zkClient).addListener(new Runnable() {
              @Override
              public void run() {
                notifyFailed(e);
              }
            }, SAME_THREAD_EXECUTOR);
          }
        }
      }, SAME_THREAD_EXECUTOR);
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
}
