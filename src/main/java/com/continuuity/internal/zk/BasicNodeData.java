package com.continuuity.internal.zk;

import com.continuuity.zk.NodeData;
import com.google.common.base.Objects;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;

/**
 *
 */
final class BasicNodeData implements NodeData {

  private final byte[] data;
  private final Stat stat;

  BasicNodeData(byte[] data, Stat stat) {
    this.data = data;
    this.stat = stat;
  }

  @Override
  public Stat getStat() {
    return stat;
  }

  @Override
  public byte[] getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof NodeData)) {
      return false;
    }

    BasicNodeData that = (BasicNodeData) o;

    return stat.equals(that.getStat()) && Arrays.equals(data, that.getData());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(data, stat);
  }
}
