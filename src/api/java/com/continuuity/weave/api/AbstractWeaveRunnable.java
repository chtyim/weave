package com.continuuity.weave.api;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public abstract class AbstractWeaveRunnable implements WeaveRunnable {

  private Map<String, String> args;

  protected AbstractWeaveRunnable() {
    this.args = ImmutableMap.of();
  }

  protected AbstractWeaveRunnable(Map<String, String> args) {
    this.args = ImmutableMap.copyOf(args);
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(getClass().getSimpleName())
      .withArguments(args)
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    this.args = context.getSpecification().getArguments();
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  protected Map<String, String> getArguments() {
    return args;
  }

  protected String getArgument(String key) {
    return args.get(key);
  }
}
