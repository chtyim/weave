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
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract base class for implementing a {@link ServiceController} using ZooKeeper as a means for
 * communicating with the remote service. This is designed to work in pair with the {@link ZKServiceDecorator}.
 */
public abstract class AbstractZKServiceController extends AbstractExecutionServiceController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractZKServiceController.class);

  private final ZKClient zkClient;
  private final InstanceNodeDataCallback instanceNodeDataCallback;

  protected AbstractZKServiceController(RunId runId, ZKClient zkClient) {
    super(runId);
    this.zkClient = zkClient;
    this.instanceNodeDataCallback = new InstanceNodeDataCallback();
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
  protected final void startUp() {
    // Watch for instance node existence.
    final AtomicBoolean instanceNodeExists = new AtomicBoolean(false);
    Futures.addCallback(zkClient.exists(getInstancePath(), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // When instance node is created, start watching the instance node.
        // Other event type would be handled in the watchInstanceNode().
        if (event.getType() == Event.EventType.NodeCreated && instanceNodeExists.compareAndSet(false, true)) {
          watchInstanceNode();
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null && instanceNodeExists.compareAndSet(false, true)) {
          watchInstanceNode();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed in exists call to instance node. Shutting down service.", t);
        stop();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    doStartUp();
  }

  protected <V> ListenableFuture<V> sendMessage(Message message, V result) {
    return ZKMessages.sendMessage(zkClient, getMessagePrefix(), message, result);
  }

  /**
   * Called during startup. Executed in the startup thread.
   */
  protected abstract void doStartUp();


  /**
   * Called when an update on the live instance node is detected.
   * @param nodeData The updated live instance node data or {@code null} if there is an error when fetching
   *                 the node data.
   */
  protected abstract void instanceNodeUpdated(NodeData nodeData);

  private void watchInstanceNode() {
    Futures.addCallback(zkClient.getData(getInstancePath(), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        State state = state();
        if (state != State.NEW && state != State.STARTING && state != State.RUNNING) {
          // Ignore ZK node events when it is in stopping sequence.
          return;
        }
        switch (event.getType()) {
          case NodeDataChanged:
            watchInstanceNode();
            break;
          case NodeDeleted:
            // When the ephemeral node goes away, treat the remote service stopped.
            stop();
            break;
          default:
            LOG.info("Ignore ZK event for instance node: {}", event);
        }
      }
    }), instanceNodeDataCallback, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Returns the path prefix for creating sequential message node for the remote service.
   */
  private String getMessagePrefix() {
    return getZKPath("messages/msg");
  }

  /**
   * Returns the zookeeper node path for the ephemeral instance node for this runId.
   */
  private String getInstancePath() {
    return String.format("/instances/%s", getRunId().getId());
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", getRunId().getId(), path);
  }

  private final class InstanceNodeDataCallback implements FutureCallback<NodeData> {

    @Override
    public void onSuccess(NodeData result) {
      instanceNodeUpdated(result);
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Failed in fetching instance node data.", t);
      if (t instanceof KeeperException && ((KeeperException) t).code() == KeeperException.Code.NONODE) {
        // If the node is gone, treat the remote service stopped.
        stop();
      } else {
        instanceNodeUpdated(null);
      }
    }
  }
}
