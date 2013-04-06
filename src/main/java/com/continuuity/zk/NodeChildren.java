package com.continuuity.zk;

import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Represents result of call to {@link com.continuuity.zk.ZKClientService#getChildren(String, org.apache.zookeeper.Watcher)} method.
 */
public interface NodeChildren {

  /**
   * @return The {@link Stat} of the node.
   */
  Stat getStat();

  /**
   * @return List of children node names.
   */
  List<String> getChildren();
}
