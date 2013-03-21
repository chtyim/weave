package com.continuuity.weave.internal.zk;

import com.google.common.util.concurrent.AbstractFuture;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

/**
 *
 */
final class SettableOperationFuture<V> extends AbstractFuture<V> implements OperationFuture<V> {

  private final String requestPath;
  private final Executor executor;

  static <V> SettableOperationFuture<V> create(String path, Executor executor) {
    return new SettableOperationFuture<V>(path, executor);
  }

  private SettableOperationFuture(String requestPath, Executor executor) {
    this.requestPath = requestPath;
    this.executor = executor;
  }

  @Override
  public String getRequestPath() {
    return requestPath;
  }

  @Override
  public void addListener(final Runnable listener, final Executor exec) {
    super.addListener(new Runnable() {
      @Override
      public void run() {
        exec.execute(listener);
      }
    }, executor);
  }

  @Override
  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }

  @Override
  public boolean set(@Nullable V value) {
    return super.set(value);
  }
}
