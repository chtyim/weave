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
package com.continuuity.internal.zk;

import com.continuuity.zk.ForwardingZKClientService;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.RetryStrategy;
import com.continuuity.zk.RetryStrategy.OperationType;
import com.continuuity.zk.ZKClientService;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link com.continuuity.zk.ZKClientService} that will invoke {@link com.continuuity.zk.RetryStrategy} on operation failure.
 * This {@link com.continuuity.zk.ZKClientService} works by delegating calls to another {@link com.continuuity.zk.ZKClientService}
 * and listen for the result. If the result is a failure, and is
 * {@link RetryUtils#canRetry(org.apache.zookeeper.KeeperException.Code) retryable}, the given {@link com.continuuity.zk.RetryStrategy}
 * will be called to determine the next retry time, or give up, depending on the value returned by the strategy.
 */
public final class FailureRetryZKClientService extends ForwardingZKClientService {

  private final RetryStrategy retryStrategy;
  private final Executor sameThreadExecutor;
  private ScheduledExecutorService scheduler;

  public FailureRetryZKClientService(ZKClientService delegate, RetryStrategy retryStrategy) {
    super(delegate);
    this.retryStrategy = retryStrategy;
    this.sameThreadExecutor = MoreExecutors.sameThreadExecutor();
  }

  @Override
  public OperationFuture<String> create(String path, byte[] data, CreateMode createMode) {
    return create(path, data, createMode, true);
  }

  @Override
  public OperationFuture<String> create(final String path, final byte[] data,
                                        final CreateMode createMode, final boolean createParent) {
    final SettableOperationFuture<String> result = SettableOperationFuture.create(path, sameThreadExecutor);
    Futures.addCallback(super.create(path, data, createMode, createParent),
                        new OperationFutureCallback<String>(OperationType.CREATE, System.currentTimeMillis(),
                                                            path, result, new Supplier<OperationFuture<String>>() {
                          @Override
                          public OperationFuture<String> get() {
                            return FailureRetryZKClientService.super.create(path, data, createMode, createParent);
                          }
                        }));
    return result;
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return exists(path, null);
  }

  @Override
  public OperationFuture<Stat> exists(final String path, final Watcher watcher) {
    final SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, sameThreadExecutor);
    Futures.addCallback(super.exists(path, watcher),
                        new OperationFutureCallback<Stat>(OperationType.EXISTS, System.currentTimeMillis(),
                                                          path, result, new Supplier<OperationFuture<Stat>>() {
                          @Override
                          public OperationFuture<Stat> get() {
                            return FailureRetryZKClientService.super.exists(path, watcher);
                          }
                        }));
    return result;
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return getChildren(path, null);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(final String path, final Watcher watcher) {
    final SettableOperationFuture<NodeChildren> result = SettableOperationFuture.create(path, sameThreadExecutor);
    Futures.addCallback(super.getChildren(path, watcher),
                        new OperationFutureCallback<NodeChildren>(OperationType.GET_CHILDREN,
                                                                  System.currentTimeMillis(), path, result,
                                                                  new Supplier<OperationFuture<NodeChildren>>() {
                          @Override
                          public OperationFuture<NodeChildren> get() {
                            return FailureRetryZKClientService.super.getChildren(path, watcher);
                          }
                        }));
    return result;
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return getData(path, null);
  }

  @Override
  public OperationFuture<NodeData> getData(final String path, final Watcher watcher) {
    final SettableOperationFuture<NodeData> result = SettableOperationFuture.create(path, sameThreadExecutor);
    Futures.addCallback(super.getData(path, watcher),
                        new OperationFutureCallback<NodeData>(OperationType.GET_DATA, System.currentTimeMillis(),
                                                              path, result, new Supplier<OperationFuture<NodeData>>() {
                          @Override
                          public OperationFuture<NodeData> get() {
                            return FailureRetryZKClientService.super.getData(path, watcher);
                          }
                        }));
    return result;
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return setData(path, data, -1);
  }

  @Override
  public OperationFuture<Stat> setData(final String dataPath, final byte[] data, final int version) {
    final SettableOperationFuture<Stat> result = SettableOperationFuture.create(dataPath, sameThreadExecutor);
    Futures.addCallback(super.setData(dataPath, data, version),
                        new OperationFutureCallback<Stat>(OperationType.SET_DATA, System.currentTimeMillis(),
                                                          dataPath, result, new Supplier<OperationFuture<Stat>>() {
                          @Override
                          public OperationFuture<Stat> get() {
                            return FailureRetryZKClientService.super.setData(dataPath, data, version);
                          }
                        }));
    return result;
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return delete(path, -1);
  }

  @Override
  public OperationFuture<String> delete(final String deletePath, final int version) {
    final SettableOperationFuture<String> result = SettableOperationFuture.create(deletePath, sameThreadExecutor);
    Futures.addCallback(super.delete(deletePath, version),
                        new OperationFutureCallback<String>(OperationType.DELETE, System.currentTimeMillis(),
                                                            deletePath, result, new Supplier<OperationFuture<String>>() {
                          @Override
                          public OperationFuture<String> get() {
                            return FailureRetryZKClientService.super.delete(deletePath, version);
                          }
                        }));
    return result;
  }

  @Override
  public ListenableFuture<State> start() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    return super.start();
  }

  @Override
  public ListenableFuture<State> stop() {
    ListenableFuture<State> result = super.stop();
    result.addListener(new Runnable() {
      @Override
      public void run() {
        scheduler.shutdownNow();
      }
    }, sameThreadExecutor);
    return result;
  }

  /**
   * Callback to watch for operation result and trigger retry if necessary.
   * @param <V> Type of operation result.
   */
  private final class OperationFutureCallback<V> implements FutureCallback<V> {

    private final OperationType type;
    private final long startTime;
    private final String path;
    private final SettableOperationFuture<V> result;
    private final Supplier<OperationFuture<V>> retryAction;
    private final AtomicInteger failureCount;

    private OperationFutureCallback(OperationType type, long startTime, String path,
                                    SettableOperationFuture<V> result, Supplier<OperationFuture<V>> retryAction) {
      this.type = type;
      this.startTime = startTime;
      this.path = path;
      this.result = result;
      this.retryAction = retryAction;
      this.failureCount = new AtomicInteger(0);
    }

    @Override
    public void onSuccess(V result) {
      this.result.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
      if (!doRetry(t)) {
        result.setException(t);
      }
    }

    private boolean doRetry(Throwable t) {
      if (!RetryUtils.canRetry(t)) {
        return false;
      }

      // Determine the relay delay
      long nextRetry = retryStrategy.nextRetry(failureCount.incrementAndGet(), startTime, type, path);
      if (nextRetry < 0) {
        return false;
      }

      // Schedule the retry.
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          Futures.addCallback(retryAction.get(), OperationFutureCallback.this);
        }
      }, nextRetry, TimeUnit.MILLISECONDS);

      return true;
    }
  }
}
