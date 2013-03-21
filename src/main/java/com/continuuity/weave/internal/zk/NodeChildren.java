package com.continuuity.weave.internal.zk;

import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 *
 */
public interface NodeChildren {

  Stat getStat();

  List<String> getChildren();
}
