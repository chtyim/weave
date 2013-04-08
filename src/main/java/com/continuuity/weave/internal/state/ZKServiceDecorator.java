package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.utils.Threads;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.RetryStrategies;
import com.continuuity.zk.ZKClientService;
import com.continuuity.zk.ZKClientServices;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.name.Named;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ZKServiceDecorator extends AbstractService {

  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
  private static Logger LOG = LoggerFactory.getLogger(ZKServiceDecorator.class);

  private final ZKClientService zkClient;
  private final String name;
  private final RunId runId;
  private final Supplier<? extends JsonElement> liveNodeData;
  private final Service decoratedService;
  private final MessageCallbackCaller messageCallback;
  private ExecutorService callbackExecutor;

  @Inject
  public ZKServiceDecorator(@Named("config.zookeeper.connect") String zkConnect,
                            @Named("config.zookeeper.timeout") int zkTimeout,
                            String name, RunId runId,
                            Supplier<? extends JsonElement> liveNodeData,
                            Service decoratedService,
                            MessageCallback messageCallback) {
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
                      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                                        .setSessionTimeout(zkTimeout)
                                                        .setConnectionWatcher(createConnectionWatcher())
                                                        .build(),
                                                      RetryStrategies.exponentialDelay(100, 2000,
                                                                                       TimeUnit.MILLISECONDS)));
    this.name = name;
    this.runId = runId;
    this.liveNodeData = liveNodeData;
    this.decoratedService = decoratedService;
    this.messageCallback = new MessageCallbackCaller(messageCallback, zkClient);
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
        listenFailure(zkClient.create(getZKPath("state"), encode(stateContent), CreateMode.PERSISTENT));
        createMessagesNode();

        // Starts the decorated service
        decoratedService.addListener(createListener(), SAME_THREAD_EXECUTOR);
        Futures.addCallback(decoratedService.start(), new FutureCallback<State>() {
          @Override
          public void onSuccess(State result) {
            notifyStarted();
          }

          @Override
          public void onFailure(Throwable t) {
            notifyFailed(t);
          }
        });
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
    final ListenableFuture<State> stopFuture = decoratedService.stop();
    stopFuture.addListener(new Runnable() {
      @Override
      public void run() {
        // Disconnect from zookeeper
        final ListenableFuture<State> zkStopFuture = zkClient.stop();
        zkStopFuture.addListener(new Runnable() {
          @Override
          public void run() {
            try {
              // Both future has to be success for it to consider stop success.
              stopFuture.get();
              zkStopFuture.get();
              notifyStopped();
            } catch (Exception e) {
              notifyFailed(e);
            } finally {
              callbackExecutor.shutdownNow();
            }
          }
        }, SAME_THREAD_EXECUTOR);
      }
    }, SAME_THREAD_EXECUTOR);
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
          LOG.info("Create live node for " + name + " " + runId);

          JsonObject content = new JsonObject();
          content.addProperty("name", name);
          content.addProperty("runId", runId.getId());
          content.add("data", liveNodeData.get());
          listenFailure(zkClient.create(getZKPath("live"), encode(content), CreateMode.EPHEMERAL));
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
    return new Listener() {
      @Override
      public void starting() {
        LOG.info("Starting: " + name + " " + runId);
      }

      @Override
      public void running() {
        LOG.info("Running: " + name + " " + runId);
      }

      @Override
      public void stopping(State from) {
        LOG.info("Stopping: " + name + " " + runId);
      }

      @Override
      public void terminated(State from) {
        LOG.info("Terminated: " + from + " " + name + " " + runId);
      }

      @Override
      public void failed(State from, Throwable failure) {
        LOG.info("Failed: " + from + " " + name + " " + runId);
      }
    };
  }

  private byte[] encode(JsonElement json) {
    return new Gson().toJson(json).getBytes(Charsets.UTF_8);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", runId.toString(), path);
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

    private MessageCallbackCaller(MessageCallback callback, ZKClientService zkClient) {
      this.callback = callback;
      this.zkClient = zkClient;
    }

    public void onReceived(Executor executor, final String path,
                           final String id, final int version, final byte[] data) {
      executor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            callback.onReceived(id, data);
          } catch (Exception e) {
            LOG.error("Failed to process message for " + id + " in " + path);
          } finally {
            listenFailure(zkClient.delete(path, version));
          }
        }
      });
    }
  }
}
