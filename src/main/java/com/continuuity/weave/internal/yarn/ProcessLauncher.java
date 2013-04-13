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
package com.continuuity.weave.internal.yarn;

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
