package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.yarn.ProcessLauncher;
import com.continuuity.weave.internal.utils.YarnUtils;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class WeaveContainerLauncher extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainerLauncher.class);

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
      LOG.info("Adding resources: " + file + " " + localRsc);
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
