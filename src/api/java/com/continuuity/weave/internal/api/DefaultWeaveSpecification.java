package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;

import java.util.Map;

/**
 *
 */
public final class DefaultWeaveSpecification implements WeaveSpecification {

  private final String name;
  private final Map<String, RuntimeSpecification> runnables;

  public DefaultWeaveSpecification(String name, Map<String, RuntimeSpecification> runnables) {
    this.name = name;
    this.runnables = runnables;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return runnables;
  }
}
