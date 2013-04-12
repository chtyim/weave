package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.Command;

/**
 *
 */
public interface Message {

  enum Scope {
    APPLICATION,
    ALL_RUNNABLE,
    RUNNABLE
  }

  String getId();

  Scope getScope();

  Command getCommand();
}
