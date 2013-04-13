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
