package com.continuuity.weave.internal.api;

import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public final class DefaultWeaveRunnableSpecification implements WeaveRunnableSpecification {

  private final String className;
  private final String name;
  private final Map<String, String> arguments;

  public DefaultWeaveRunnableSpecification(String className, String name, Map<String, String> arguments) {
    this.className = className;
    this.name = name;
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  public DefaultWeaveRunnableSpecification(String className, WeaveRunnableSpecification other) {
    this.className = className;
    this.name = other.getName();
    this.arguments = ImmutableMap.copyOf(other.getArguments());
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, String> getArguments() {
    return arguments;
  }
}
