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
