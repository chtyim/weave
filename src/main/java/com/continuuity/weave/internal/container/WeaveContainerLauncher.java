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
package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.utils.YarnUtils;
import com.continuuity.weave.internal.yarn.ProcessLauncher;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

import java.io.File;

/**
 *
 */
public class WeaveContainerLauncher extends AbstractIdleService {

  private final WeaveSpecification weaveSpec;
  private final File weaveSpecFile;
  private final String runnableName;
  private final ProcessLauncher processLauncher;
  private final String zkConnectStr;
  private ProcessLauncher.ProcessController controller;

  public WeaveContainerLauncher(WeaveSpecification weaveSpec,
                                File weaveSpecFile,
                                String runnableName,
                                ProcessLauncher processLauncher,
                                String zkConnectStr) {
    this.weaveSpec = weaveSpec;
    this.weaveSpecFile = weaveSpecFile;
    this.runnableName = runnableName;
    this.processLauncher = processLauncher;
    this.zkConnectStr = zkConnectStr;
  }

  @Override
  protected void startUp() throws Exception {
    RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);

    ProcessLauncher.PrepareLaunchContext.AfterUser afterUser = processLauncher.prepareLaunch()
      .setUser(System.getProperty("user.name"));

    ProcessLauncher.PrepareLaunchContext.MoreResources moreResources =
      afterUser.withResources().add("weave.spec", YarnUtils.createLocalResource(LocalResourceType.FILE, weaveSpecFile));

    for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
      File file = new File(runnableName + "." + localFile.getName());
      LocalResource localRsc = setLocalResourceType(localFile,
                                                    YarnUtils.createLocalResource(LocalResourceType.FILE, file));
      moreResources = moreResources.add(localFile.getName(), localRsc);
    }

    controller = moreResources.withCommands()
      .add("java",
           "com.continuuity.weave.internal.container.WeaveContainerMain",
           zkConnectStr,
           "weave.spec",
           runnableName)
      .redirectOutput("/tmp/container." + runnableName + ".out")
      .redirectError("/tmp/container." + runnableName + ".err")
      .launch();
  }

  @Override
  protected void shutDown() throws Exception {
    controller.stop();
  }

  private LocalResource setLocalResourceType(LocalFile localFile, LocalResource localResource) {
    if (localFile.isArchive()) {
      if (localFile.getPattern() == null) {
        localResource.setType(LocalResourceType.ARCHIVE);
      } else {
        localResource.setType(LocalResourceType.PATTERN);
        localResource.setPattern(localFile.getPattern());
      }
    } else {
      localResource.setType(LocalResourceType.FILE);
    }
    return localResource;
  }
}
