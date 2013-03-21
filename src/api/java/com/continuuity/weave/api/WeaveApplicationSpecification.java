package com.continuuity.weave.api;

import java.util.Map;

/**
 *
 */
public interface WeaveApplicationSpecification {

  String getName();

  Map<String, WeaveSpecification> getRunnables();


  public static final class Builder {

    private String name;

    public static Builder with() {
      return new Builder();
    }

    public AfterName setName(String name) {
      this.name = name;
      return new AfterName();
    }

    public final class AfterName {
      public MoreRunnable withRunnable() {
        return new RunnableSetter();
      }
    }

    public interface MoreRunnable {
      RunnableSetter add(WeaveRunnable runnable);


    }

    public interface AfterRunnable {

    }

    public final class RunnableSetter implements MoreRunnable, AfterRunnable {

      @Override
      public RunnableSetter add(WeaveRunnable runnable) {


        return this;
      }
    }

    private Builder() {}
  }
}
