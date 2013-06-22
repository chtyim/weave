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
package com.continuuity.weave.zookeeper;

import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.zookeeper.SettableOperationFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collection of helper methods for common operations that usually needed when interacting with ZooKeeper.
 */
public final class ZKOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ZKOperations.class);

  /**
   * Represents a ZK operation updates callback.
   * @param <T> Type of updated data.
   */
  public interface Callback<T> {
    void updated(T data);
  }

  /**
   * Interface for defining callback method to receive node data updates.
   */
  public interface DataCallback extends Callback<NodeData> {
    /**
     * Invoked when data of the node changed.
     * @param nodeData New data of the node, or {@code null} if the node has been deleted.
     */
    @Override
    void updated(NodeData nodeData);
  }

  /**
   * Interface for defining callback method to receive children nodes updates.
   */
  public interface ChildrenCallback extends Callback<NodeChildren> {
    @Override
    void updated(NodeChildren nodeChildren);
  }

  private interface Operation<T> {
    ZKClient getZKClient();

    OperationFuture<T> exec(String path, Watcher watcher);
  }

  /**
   * Watch for data changes of the given path. The callback will be triggered whenever changes has been
   * detected. Note that the callback won't see every single changes, as that's not the guarantee of ZooKeeper.
   * If the node doesn't exists, it will watch for its creation then starts watching for data changes.
   * When the node is deleted afterwards,
   *
   * @param zkClient The {@link ZKClient} for the operation
   * @param path Path to watch
   * @param callback Callback to be invoked when data changes is detected.
   * @return A {@link Cancellable} to cancel the watch.
   */
  public static Cancellable watchData(final ZKClient zkClient, final String path, final DataCallback callback) {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    watchChanges(new Operation<NodeData>() {

      @Override
      public ZKClient getZKClient() {
        return zkClient;
      }

      @Override
      public OperationFuture<NodeData> exec(String path, Watcher watcher) {
        return zkClient.getData(path, watcher);
      }
    }, path, callback, cancelled);

    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  public static ListenableFuture<String> watchDeleted(final ZKClient zkClient, final String path) {
    SettableFuture<String> completion = SettableFuture.create();
    watchDeleted(zkClient, path, completion);
    return completion;
  }

  public static void watchDeleted(final ZKClient zkClient, final String path,
                                  final SettableFuture<String> completion) {

    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          if (event.getType() == Event.EventType.NodeDeleted) {
            completion.set(path);
          } else {
            watchDeleted(zkClient, path, completion);
          }
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result == null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    });
  }

  public static Cancellable watchChildren(final ZKClient zkClient, String path, ChildrenCallback callback) {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    watchChanges(new Operation<NodeChildren>() {

      @Override
      public ZKClient getZKClient() {
        return zkClient;
      }

      @Override
      public OperationFuture<NodeChildren> exec(String path, Watcher watcher) {
        return zkClient.getChildren(path, watcher);
      }
    }, path, callback, cancelled);

    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  /**
   * Returns a new {@link OperationFuture} that the result will be the same as the given future, except that when
   * the source future is having an exception matching the giving exception type, the errorResult will be set
   * in to the returned {@link OperationFuture}.
   * @param future The source future.
   * @param exceptionType Type of {@link KeeperException} to be ignored.
   * @param errorResult Object to be set into the resulting future on a matching exception.
   * @param <V> Type of the result.
   * @return A new {@link OperationFuture}.
   */
  public static <V> OperationFuture<V> ignoreError(OperationFuture<V> future,
                                                   final Class<? extends KeeperException> exceptionType,
                                                   final V errorResult) {
    final SettableOperationFuture<V> resultFuture = SettableOperationFuture.create(future.getRequestPath(),
                                                                                   Threads.SAME_THREAD_EXECUTOR);

    Futures.addCallback(future, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (exceptionType.isAssignableFrom(t.getClass())) {
          resultFuture.set(errorResult);
        } else if (t instanceof CancellationException) {
          resultFuture.cancel(true);
        } else {
          resultFuture.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return resultFuture;
  }

  /**
   * Watch for the given path until it exists.
   * @param zkClient The {@link ZKClient} to use.
   * @param path A ZooKeeper path to watch for existent.
   */
  private static void watchExists(final ZKClient zkClient, final String path, final SettableFuture<String> completion) {
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          watchExists(zkClient, path, completion);
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    });
  }

  private static <T> void watchChanges(final Operation<T> operation, final String path,
                                       final Callback<T> callback, final AtomicBoolean cancelled) {
    Futures.addCallback(operation.exec(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!cancelled.get()) {
          watchChanges(operation, path, callback, cancelled);
        }
      }
    }), new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        if (!cancelled.get()) {
          callback.updated(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException && ((KeeperException) t).code() == KeeperException.Code.NONODE) {
          final SettableFuture<String> existCompletion = SettableFuture.create();
          existCompletion.addListener(new Runnable() {
            @Override
            public void run() {
              try {
                if (!cancelled.get()) {
                  watchChanges(operation, existCompletion.get(), callback, cancelled);
                }
              } catch (Exception e) {
                LOG.error("Failed to watch children for path " + path, e);
              }
            }
          }, Threads.SAME_THREAD_EXECUTOR);
          watchExists(operation.getZKClient(), path, existCompletion);
          return;
        }
        LOG.error("Failed to watch data for path " + path + " " + t, t);
      }
    });
  }

  private ZKOperations() {
  }
}
