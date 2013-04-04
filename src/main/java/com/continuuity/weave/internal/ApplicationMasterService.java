package com.continuuity.weave.internal;

import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.container.WeaveContainerLauncher;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  private final String zkConnectStr;
  private final WeaveSpecification weaveSpec;
  private final File weaveSpecFile;
  private final YarnConfiguration yarnConf;
  private final AMRMClient amrmClient;
  private final Queue<WeaveContainerLauncher> launchers;
  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;

  private volatile Thread runThread;


  public ApplicationMasterService(String zkConnectStr, WeaveSpecification weaveSpec, File weaveSpecFile) {
    this.zkConnectStr = zkConnectStr;
    this.weaveSpec = weaveSpec;
    this.weaveSpecFile = weaveSpecFile;

    this.yarnConf = new YarnConfiguration();
    this.launchers = new ConcurrentLinkedQueue<WeaveContainerLauncher>();

    // Get the container ID and convert it to ApplicationAttemptId
    String containerIdString = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(containerIdString != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(containerIdString).getApplicationAttemptId());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Start application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    yarnRPC = YarnRPC.create(yarnConf);

    amrmClient.init(yarnConf);
    amrmClient.start();
    // TODO: Have RPC host and port
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, null);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();
  }

  @Override
  protected void shutDown() throws Exception {
    for (WeaveContainerLauncher launcher : launchers) {
      launcher.stopAndWait();
    }

    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    amrmClient.stop();
  }

  @Override
  protected void triggerShutdown() {
    Thread runThread = this.runThread;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void run() throws Exception {
    runThread = Thread.currentThread();

    // TODO: We should be able to declare service start sequence
    Queue<RuntimeSpecification> runtimeSpecs = Lists.newLinkedList();

    // Simply goes through the spec and create container requests
    for (Map.Entry<String,RuntimeSpecification> entry : weaveSpec.getRunnables().entrySet()) {
      RuntimeSpecification runtimeSpec = entry.getValue();

      Resource capability = Records.newRecord(Resource.class);
      capability.setVirtualCores(runtimeSpec.getResourceSpecification().getCores());
      capability.setMemory(runtimeSpec.getResourceSpecification().getMemorySize());

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority, 1);
      amrmClient.addContainerRequest(request);

      runtimeSpecs.add(entry.getValue());
    }

    while (isRunning()) {
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      AMResponse amResponse = allocateResponse.getAMResponse();
      List<Container> containers = amResponse.getAllocatedContainers();

      LOG.info("Containers size: " + containers.size());
      LOG.info("Containers: " + containers);

      // TODO: Match the resource capability.
      for (Container container : containers) {
        RuntimeSpecification runtimeSpec = runtimeSpecs.poll();
        DefaultProcessLauncher processLauncher = new DefaultProcessLauncher(container, yarnRPC, yarnConf);
        WeaveContainerLauncher launcher = new WeaveContainerLauncher(weaveSpec, weaveSpecFile, runtimeSpec.getName(),
                                                                     processLauncher, zkConnectStr);
        launcher.start();
        launchers.add(launcher);
      }

      TimeUnit.SECONDS.sleep(1);
    }
  }
}
