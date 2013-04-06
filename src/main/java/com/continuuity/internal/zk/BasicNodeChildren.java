package com.continuuity.internal.zk;

import com.continuuity.zk.NodeChildren;
import com.google.common.base.Objects;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 *
 */
final class BasicNodeChildren implements NodeChildren {

  private final Stat stat;
  private final List<String> children;

  BasicNodeChildren(List<String> children, Stat stat) {
    this.stat = stat;
    this.children = children;
  }

  @Override
  public Stat getStat() {
    return stat;
  }

  @Override
  public List<String> getChildren() {
    return children;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof NodeChildren)) {
      return false;
    }

    NodeChildren that = (NodeChildren) o;
    return stat.equals(that.getStat()) && children.equals(that.getChildren());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(children, stat);
  }
}
