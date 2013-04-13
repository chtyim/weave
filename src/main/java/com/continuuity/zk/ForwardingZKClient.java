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
package com.continuuity.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 *
 */
public abstract class ForwardingZKClient implements ZKClient {

  private final ZKClient delegate;

  protected ForwardingZKClient(ZKClient delegate) {
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
}
