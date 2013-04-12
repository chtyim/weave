package com.continuuity.zk;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.Executor;

/**
 *
 */
public abstract class ForwardingZKClientService extends ForwardingZKClient implements ZKClientService {

  private final ZKClientService delegate;

  protected ForwardingZKClientService(ZKClientService delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public Supplier<ZooKeeper> getZooKeeperSupplier() {
    return delegate.getZooKeeperSupplier();
  }

  @Override
  public ListenableFuture<State> start() {
    return delegate.start();
  }

  @Override
  public State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public boolean isRunning() {
    return delegate.isRunning();
  }

  @Override
  public State state() {
    return delegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return delegate.stop();
  }

  @Override
  public State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    delegate.addListener(listener, executor);
  }
}
