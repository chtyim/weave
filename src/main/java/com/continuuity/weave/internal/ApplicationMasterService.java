package com.continuuity.weave.internal;

import com.continuuity.weave.api.WeaveApplicationSpecification;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
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

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService extends AbstractExecutionThreadService {

  private final String zkConnectStr;
  private final WeaveApplicationSpecification appSpec;
  private final YarnConfiguration yarnConf;
  private final AMRMClient amrmClient;
  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;

  private volatile Thread runThread;


  public ApplicationMasterService(String zkConnectStr, WeaveApplicationSpecification appSpec) {
    this.zkConnectStr = zkConnectStr;
    this.appSpec = appSpec;

    this.yarnConf = new YarnConfiguration();

    // Get the container ID and convert it to ApplicationAttemptId
    String containerIdString = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(containerIdString != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(containerIdString).getApplicationAttemptId());
  }

  @Override
  protected void startUp() throws Exception {
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

    final PrintWriter writer = new PrintWriter(Files.newWriter(new File("/tmp/appout"), Charsets.UTF_8), true);
    try {
      // Simply goes through the spec and create container requests
  //    for (Map.Entry<String, WeaveSpecification> entry : appSpec.getRunnables().entrySet()) {
  //      WeaveSpecification weaveSpec = entry.getValue();
  //      Resource resource = Records.newRecord(Resource.class);
  //      // TODO:
  //      resource.setVirtualCores(1);
  //      resource.setMemory(512);  // in mb
  //
  //      Priority priority = Records.newRecord(Priority.class);
  //
  //      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(resource, null, null, priority, 1);
  //      amrmClient.addContainerRequest(request);
  //    }

      // Kafka Container
      Resource capability = Records.newRecord(Resource.class);
      // TODO:
      capability.setVirtualCores(1);
      capability.setMemory(512);  // in mb

      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority, 1);
      amrmClient.addContainerRequest(request);

      KafkaContainerManager kafkaManager = null;
      while (isRunning()) {
        AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
        AMResponse amResponse = allocateResponse.getAMResponse();
        List<Container> allocatedContainers = amResponse.getAllocatedContainers();
        if (!allocatedContainers.isEmpty()) {
          // Got the kafka container
          kafkaManager = new KafkaContainerManager(zkConnectStr,
                                                   new DefaultProcessLauncher(allocatedContainers.get(0),
                                                                              yarnRPC, yarnConf));
          break;
        }

        TimeUnit.SECONDS.sleep(1);
      }

      kafkaManager.start();

      while (isRunning()) {
        AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
        AMResponse amResponse = allocateResponse.getAMResponse();
        TimeUnit.SECONDS.sleep(1);
      }
    } finally {
      writer.close();
    }
  }



}
