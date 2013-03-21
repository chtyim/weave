package com.continuuity.weave.internal.zk;

import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 *
 */
public interface NodeData {

  Stat getStat();

  @Nullable
  byte[] getData();
}
