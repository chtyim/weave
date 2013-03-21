package com.continuuity.weave.internal.zk;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

/**
 *
 */
abstract class ForwardingZKClientService implements ZKClientService {

  private final ZKClientService delegate;

  protected ForwardingZKClientService(ZKClientService delegate) {
    this.delegate = delegate;
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    return delegate.create(path, data, createMode);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                        boolean createParent) {
    return delegate.create(path, data, createMode, createParent);
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return delegate.exists(path);
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return delegate.exists(path, watcher);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return delegate.getChildren(path);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return delegate.getChildren(path, watcher);
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return delegate.getData(path);
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return delegate.getData(path, watcher);
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return delegate.setData(path, data);
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return delegate.setData(dataPath, data, version);
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return delegate.delete(path);
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return delegate.delete(deletePath, version);
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
