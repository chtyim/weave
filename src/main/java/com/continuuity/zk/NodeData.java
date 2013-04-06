package com.continuuity.zk;

import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * Represents result of call to {@link com.continuuity.zk.ZKClientService#getData(String, org.apache.zookeeper.Watcher)}.
 */
public interface NodeData {

  /**
   * @return The {@link Stat} of the node.
   */
  Stat getStat();

  /**
   * @return Data stored in the node, or {@code null} if there is no data.
   */
  @Nullable
  byte[] getData();
}
