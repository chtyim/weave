package com.continuuity.weave.api;

import com.continuuity.weave.internal.api.DefaultWeaveRunnableSpecification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public interface WeaveRunnableSpecification {

  String getClassName();

  String getName();

  Map<String, String> getArguments();

  public static final class Builder {

    private String name;
    private Map<String, String> args;

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      public AfterName setName(String name) {
        Builder.this.name = name;
        return new AfterName();
      }
    }

    public final class AfterName {
      public AfterArguments withArguments(Map<String, String> args) {
        Builder.this.args = args;
        return new AfterArguments();
      }

      public AfterArguments noArguments() {
        Builder.this.args = ImmutableMap.of();
        return new AfterArguments();
      }
    }

    public final class AfterArguments {
      public WeaveRunnableSpecification build() {
        return new DefaultWeaveRunnableSpecification(null, name, args);
      }
    }

    private Builder() {
    }
  }
}
