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
package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DefaultWeaveSpecification implements WeaveSpecification {

  private final String name;
  private final Map<String, RuntimeSpecification> runnables;
  private final List<Order> orders;

  public DefaultWeaveSpecification(String name, Map<String, RuntimeSpecification> runnables,
                                   List<Order> orders) {
    this.name = name;
    this.runnables = ImmutableMap.copyOf(runnables);
    this.orders = ImmutableList.copyOf(orders);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return runnables;
  }

  @Override
  public List<Order> getOrders() {
    return orders;
  }

  public static final class DefaultOrder implements Order {

    private final Set<String> names;
    private final Type type;

    public DefaultOrder(Iterable<String> names, Type type) {
      this.names = ImmutableSet.copyOf(names);
      this.type = type;
    }

    @Override
    public Set<String> getNames() {
      return names;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("names", names)
        .add("type", type)
        .toString();
    }
  }
}
