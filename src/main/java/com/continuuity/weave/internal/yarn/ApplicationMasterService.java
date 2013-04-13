package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.container.WeaveContainerLauncher;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService implements Service {

  private static final int ZK_TIMEOUT = 10000;    // 10 seconds
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  private final RunId runId;
  private final String zkConnectStr;
  private final WeaveSpecification weaveSpec;
  private final File weaveSpecFile;
  private final YarnConfiguration yarnConf;
  private final String masterContainerId;
  private final AMRMClient amrmClient;
  private final Queue<WeaveContainerLauncher> launchers;
  private final ZKServiceDecorator serviceDelegate;
  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;


  public ApplicationMasterService(String zkConnectStr, File weaveSpecFile) throws IOException {
    this.zkConnectStr = zkConnectStr;
    this.weaveSpecFile = weaveSpecFile;
    this.weaveSpec = WeaveSpecificationAdapter.create().fromJson(weaveSpecFile);

    this.yarnConf = new YarnConfiguration();
    this.launchers = new ConcurrentLinkedQueue<WeaveContainerLauncher>();

    this.runId = RunIds.generate();
    this.serviceDelegate = new ZKServiceDecorator(zkConnectStr, ZK_TIMEOUT, runId,
                                                  createLiveNodeDataSupplier(), new ServiceDelegate());

    // Get the container ID and convert it to ApplicationAttemptId
    masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(masterContainerId).getApplicationAttemptId());
  }

  private Supplier<? extends JsonElement> createLiveNodeDataSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("containerId", masterContainerId);
        return jsonObj;
      }
    };
  }

  private void doStart() throws Exception {
    LOG.info("Start application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    yarnRPC = YarnRPC.create(yarnConf);

    amrmClient.init(yarnConf);
    amrmClient.start();
    // TODO: Have RPC host and port
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, null);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();

    LOG.info("Maximum resource capability: " + maxCapability);
    LOG.info("Minimum resource capability: " + minCapability);

    serviceDelegate.getZKClient().create("/" + runId + "/runnables", null, CreateMode.PERSISTENT).get();
  }

  private void doStop() throws Exception {
    for (WeaveContainerLauncher launcher : launchers) {
      launcher.stopAndWait();
    }

    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    amrmClient.stop();
  }


  private void doRun() throws Exception {
    // Based on the startup sequence, starts the containers
//    for (WeaveSpecification.Order order : weaveSpec.getOrders()) {
//      order.getType()
//    }

    // TODO: We should be able to declare service start sequence
    final List<RunnableContainerRequest> runtimeSpecs = Lists.newLinkedList();

    // Simply goes through the spec and create container requests
    for (Map.Entry<String,RuntimeSpecification> entry : weaveSpec.getRunnables().entrySet()) {
      RuntimeSpecification runtimeSpec = entry.getValue();

      Resource capability = createCapability(runtimeSpec.getResourceSpecification());

      // TODO: Allow user to set priority?
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority, 1);
      amrmClient.addContainerRequest(request);

      runtimeSpecs.add(new RunnableContainerRequest(entry.getValue().getName(), capability));
    }

    while (isRunning()) {
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      allocateResponse.getAMResponse().getResponseId();
      AMResponse amResponse = allocateResponse.getAMResponse();
      List<Container> containers = amResponse.getAllocatedContainers();

      for (final Container container : containers) {
        LOG.info("Container provisioned: " + container);

        // For each container provisioned, find the request that has the resource capability matched.
        boolean executed = false;
        Iterator<RunnableContainerRequest> requestItor = runtimeSpecs.iterator();
        while (requestItor.hasNext()) {
          RunnableContainerRequest request = requestItor.next();
          LOG.info("Requested capability: " + request.getCapability());
          // TODO: Find way to tie request to resource
//          if (!container.getResource().equals(request.getCapability())) {
//            continue;
//          }
          requestItor.remove();
          DefaultProcessLauncher processLauncher = new DefaultProcessLauncher(container, yarnRPC, yarnConf);
          WeaveContainerLauncher launcher = new WeaveContainerLauncher(weaveSpec, weaveSpecFile, request.getName(),
                                                                       processLauncher,
                                                                       getRunnableZKConnectStr(request.getName()));
          launcher.start();
          launchers.add(launcher);
          executed = true;
          break;
        }

        if (!executed) {
          LOG.info("Nothing to run in container, releasing it: " + container);
          amrmClient.releaseAssignedContainer(container.getId());
        }
      }

      TimeUnit.SECONDS.sleep(1);
    }
  }

  private String getRunnableZKConnectStr(String runnableName) {
    return String.format("%s/%s/runnables/%s", zkConnectStr, runId, runnableName);
  }

  private ListenableFuture<String> processMessage(Message message) {
    SettableFuture<String> result = SettableFuture.create();

    return result;
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    int cores = Math.max(Math.min(resourceSpec.getCores(), maxCapability.getVirtualCores()),
                         minCapability.getVirtualCores());
    capability.setVirtualCores(cores);

    int memory = Math.max(Math.min(resourceSpec.getMemorySize(), maxCapability.getMemory()),
                          minCapability.getMemory());
    capability.setMemory(memory);

    return capability;
  }

  @Override
  public ListenableFuture<State> start() {
    return serviceDelegate.start();
  }

  @Override
  public State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public State state() {
    return serviceDelegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  @Override
  public State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    serviceDelegate.addListener(listener, executor);
  }

  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

    @Override
    protected void run() throws Exception {
      runThread = Thread.currentThread();
      try {
        doRun();
      } catch (InterruptedException e) {
        // It's ok to get interrupted exception
        Thread.currentThread().interrupt();
      }
    }

    @Override
    protected void startUp() throws Exception {
      doStart();
    }

    @Override
    protected void shutDown() throws Exception {
      doStop();
    }

    @Override
    protected void triggerShutdown() {
      Thread runThread = this.runThread;
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public ListenableFuture<String> onReceived(Message message) {
      return processMessage(message);
    }
  }

  private static final class RunnableContainerRequest {
    private final String name;
    private final Resource capability;

    private RunnableContainerRequest(String name, Resource capability) {
      this.name = name;
      this.capability = capability;
    }

    private String getName() {
      return name;
    }

    private Resource getCapability() {
      return capability;
    }
  }
}
