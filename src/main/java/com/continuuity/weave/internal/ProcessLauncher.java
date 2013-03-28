package com.continuuity.weave.internal;

import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 *
 */
public interface ProcessLauncher {

  PrepareLaunchContext prepareLaunch();

  interface ProcessController {
    void stop();

    void status();
  }

  interface PrepareLaunchContext {

    interface AfterUser {
      ResourcesAdder withResources();

      AfterResources noResources();
    }

    interface ResourcesAdder {
      MoreResources add(String name, LocalResource resource);
    }

    interface AfterResources {
      CommandAdder withCommands();
    }

    interface MoreResources extends ResourcesAdder, AfterResources { }

    interface CommandAdder {
      StdOutSetter add(String cmd, String...args);
    }

    interface StdOutSetter {
      StdErrSetter redirectOutput(String stdout);

      StdErrSetter noOutput();
    }

    interface StdErrSetter {
      MoreCommand redirectError(String stderr);

      MoreCommand noError();
    }

    interface MoreCommand extends CommandAdder {
      ProcessController launch();
    }

    AfterUser setUser(String user);
  }
}
