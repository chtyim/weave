/*
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
package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.filesystem.HDFSLocationFactory;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.Arguments;
import com.continuuity.weave.internal.EnvKeys;
import com.continuuity.weave.internal.ProcessLauncher;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.WeaveContainerLauncher;
import com.continuuity.weave.internal.json.ArgumentsCodec;
import com.continuuity.weave.internal.json.LocalFileCodec;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.ZKServiceDecorator;
import com.continuuity.weave.internal.utils.Networks;
import com.continuuity.weave.internal.yarn.ports.AMRMClient;
import com.continuuity.weave.internal.yarn.ports.AMRMClientImpl;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  private final RunId runId;
  private final ZKClient zkClient;
  private final WeaveSpecification weaveSpec;
  private final Multimap<String, String> runnableArgs;
  private final YarnConfiguration yarnConf;
  private final String masterContainerId;
  private final AMRMClient amrmClient;
  private final ZKServiceDecorator serviceDelegate;
  private final RunningContainers runningContainers;
  private final Map<String, Integer> instanceCounts;

  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;
  private EmbeddedKafkaServer kafkaServer;
  private Queue<RunnableContainerRequest> runnableContainerRequests;
  private ExecutorService instanceChangeExecutor;


  public ApplicationMasterService(RunId runId, ZKClient zkClient, File weaveSpecFile) throws IOException {
    this.runId = runId;
    this.weaveSpec = WeaveSpecificationAdapter.create().fromJson(weaveSpecFile);
    this.runnableArgs = decodeRunnableArgs();
    this.yarnConf = new YarnConfiguration();
    this.zkClient = zkClient;

    // Get the container ID and convert it to ApplicationAttemptId
    masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(masterContainerId).getApplicationAttemptId());

    runningContainers = new RunningContainers();

    serviceDelegate = new ZKServiceDecorator(zkClient, runId, createLiveNodeDataSupplier(), new ServiceDelegate());
    instanceCounts = initInstanceCounts(weaveSpec, Maps.<String, Integer>newConcurrentMap());
  }

  private Multimap<String, String> decodeRunnableArgs() throws IOException {
    BufferedReader reader = Files.newReader(new File("arguments.json"), Charsets.UTF_8);
    try {
      return new GsonBuilder().registerTypeAdapter(Arguments.class, new ArgumentsCodec())
        .create().fromJson(reader, Arguments.class).getRunnableArguments();
    } finally {
      reader.close();
    }
  }

  private Supplier<? extends JsonElement> createLiveNodeDataSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("appId", Integer.parseInt(System.getenv(EnvKeys.WEAVE_APP_ID)));
        jsonObj.addProperty("appIdClusterTime", Long.parseLong(System.getenv(EnvKeys.WEAVE_APP_ID_CLUSTER_TIME)));
        jsonObj.addProperty("containerId", masterContainerId);
        return jsonObj;
      }
    };
  }

  private Map<String, Integer> initInstanceCounts(WeaveSpecification weaveSpec, Map<String, Integer> result) {
    for (RuntimeSpecification runtimeSpec : weaveSpec.getRunnables().values()) {
      result.put(runtimeSpec.getName(), runtimeSpec.getResourceSpecification().getInstances());
    }
    return result;
  }

  private void doStart() throws Exception {
    LOG.info("Start application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    instanceChangeExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("instanceChanger"));
    yarnRPC = YarnRPC.create(yarnConf);

    amrmClient.init(yarnConf);
    amrmClient.start();
    // TODO: Have RPC host and port
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, null);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();

    LOG.info("Maximum resource capability: " + maxCapability);
    LOG.info("Minimum resource capability: " + minCapability);

    // Creates ZK path for runnable and kafka logging service
    Futures.allAsList(ImmutableList.of(
      zkClient.create("/" + runId.getId() + "/runnables", null, CreateMode.PERSISTENT),
      zkClient.create("/" + runId.getId() + "/kafka", null, CreateMode.PERSISTENT))
    ).get();

    // Starts kafka server
    LOG.info("Starting kafka server");
    kafkaServer = new EmbeddedKafkaServer(new File("kafka.tgz"), generateKafkaConfig());
    kafkaServer.startAndWait();
    LOG.info("Kafka server started");

    runnableContainerRequests = initContainerRequests();
  }

  private void doStop() throws Exception {
    Thread.interrupted();     // This is just to clear the interrupt flag

    LOG.info("Stop application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    instanceChangeExecutor.shutdownNow();

    Set<ContainerId> ids = Sets.newHashSet(runningContainers.getContainerIds());
    runningContainers.stopAll();

    int count = 0;
    while (!ids.isEmpty() && count++ < 5) {
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      for (ContainerStatus status : allocateResponse.getAMResponse().getCompletedContainersStatuses()) {
        ids.remove(status.getContainerId());
      }
      TimeUnit.SECONDS.sleep(1);
    }

    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    amrmClient.stop();

    // App location cleanup
    cleanupDir(URI.create(System.getenv(EnvKeys.WEAVE_APP_DIR)));

    // When logger context is stopped, stop the kafka server as well.
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (loggerFactory instanceof LoggerContext) {
      ((LoggerContext) loggerFactory).addListener(getLoggerStopListener());
    } else {
      kafkaServer.stopAndWait();
    }
  }

  private void cleanupDir(URI appDir) {
    // Cleanup based on the uri schema.
    // Note: It's a little bit hacky, refactor it later.
    Location location;
    if ("file".equals(appDir.getScheme())) {
      location = new LocalLocationFactory().create(appDir);
    } else if ("hdfs".equals(appDir.getScheme())) {
      location = new HDFSLocationFactory(yarnConf).create(appDir);
    } else {
      LOG.warn("Unsupport location type {}. Cleanup not performed.", appDir);
      return;
    }

    try {
      if (location.delete(true)) {
        LOG.info("Application directory deleted: {}", appDir);
      } else {
        LOG.warn("Failed to cleanup directory {}.", appDir);
      }
    } catch (IOException e) {
      LOG.warn("Exception while cleanup directory {}.", appDir, e);
    }
  }

  private LoggerContextListener getLoggerStopListener() {
    return new LoggerContextListenerAdapter(true) {
      @Override
      public void onStop(LoggerContext context) {
        // Sleep a bit before stopping kafka to have chance for client to fetch the last log.
        try {
          TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
          // Ignored.
        } finally {
          try {
            kafkaServer.stop().get(5, TimeUnit.SECONDS);
          } catch (Exception e) {
            // Cannot use logger here
            e.printStackTrace(System.err);
          }
        }
      }
    };
  }

  private void doRun() throws Exception {
    // The main loop
    Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> currentRequest = null;
    Queue<ProvisionRequest> provisioning = Lists.newLinkedList();
    while (isRunning()) {
      // If nothing is in provisioning, and no pending request, move to next one
      while (provisioning.isEmpty() && currentRequest == null && !runnableContainerRequests.isEmpty()) {
        currentRequest = runnableContainerRequests.peek().takeRequest();
        if (currentRequest == null) {
          // All different types of resource request from current order is done, move to next one
          // TODO: Need to handle order type as well
          runnableContainerRequests.poll();
        }
      }
      // Nothing in provision, makes the next batch of provision request
      if (provisioning.isEmpty() && currentRequest != null) {
        provisioning.addAll(addContainerRequests(currentRequest.getKey(), currentRequest.getValue()));
        currentRequest = null;
      }

      // Now call allocate
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      allocateResponse.getAMResponse().getResponseId();
      AMResponse amResponse = allocateResponse.getAMResponse();

      // Assign runnable to container
      launchRunnable(amResponse.getAllocatedContainers(), provisioning);
      handleCompleted(amResponse.getCompletedContainersStatuses());

      if (provisioning.isEmpty() && runnableContainerRequests.isEmpty() && runningContainers.isEmpty()) {
        LOG.info("All containers completed. Shutting down application master.");
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
  }

  /**
   * Handling containers that are completed.
   */
  private void handleCompleted(List<ContainerStatus> completedContainersStatuses) {
    for (ContainerStatus status : completedContainersStatuses) {
      runningContainers.handleCompleted(status.getContainerId(), status.getExitStatus());
    }
  }

  private Queue<RunnableContainerRequest> initContainerRequests() {
    // Orderly stores container requests.
    Queue<RunnableContainerRequest> requests = Lists.newLinkedList();
    // For each order in the weaveSpec, create container request for each runnable.
    for (WeaveSpecification.Order order : weaveSpec.getOrders()) {
      // Group container requests based on resource requirement.
      ImmutableMultimap.Builder<Resource, RuntimeSpecification> builder = ImmutableMultimap.builder();
      for (String runnableName : order.getNames()) {
        RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        builder.put(capability, runtimeSpec);
      }
      requests.add(new RunnableContainerRequest(order.getType(), builder.build()));
    }
    return requests;
  }

  /**
   * Adds {@link AMRMClient.ContainerRequest}s with the given resource capability for each runtime.
   */
  private Queue<ProvisionRequest> addContainerRequests(Resource capability,
                                                       Collection<RuntimeSpecification> runtimeSpecs) {
    Queue<ProvisionRequest> requests = Lists.newLinkedList();
    for (RuntimeSpecification runtimeSpec : runtimeSpecs) {
      // TODO: Allow user to set priority?
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      String name = runtimeSpec.getName();
      int containerCount = instanceCounts.get(name) - runningContainers.count(name);
      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
                                                                            priority, containerCount);
      LOG.info("Request for container: " + request);
      requests.add(new ProvisionRequest(request, runtimeSpec, runningContainers.getBaseRunId(name)));
      amrmClient.addContainerRequest(request);
    }
    return requests;
  }

  /**
   * Launches runnables in the provisioned containers.
   */
  private void launchRunnable(List<Container> containers, Queue<ProvisionRequest> provisioning) {
    for (Container container : containers) {
      ProvisionRequest provisionRequest = provisioning.peek();
      if (provisionRequest == null) {
        LOG.info("Nothing to run in container, releasing it: " + container);
        amrmClient.releaseAssignedContainer(container.getId());
        continue;
      }

      String runnableName = provisionRequest.getRuntimeSpec().getName();
      int containerCount = instanceCounts.get(provisionRequest.getRuntimeSpec().getName());
      int instanceId = runningContainers.count(runnableName);

      RunId containerRunId = RunIds.fromString(provisionRequest.getBaseRunId().getId() + "-" + instanceId);
      ProcessLauncher processLauncher = new DefaultProcessLauncher(
        container, yarnRPC, yarnConf, getLocalFiles(),
        ImmutableMap.<String, String>builder()
         .put(EnvKeys.WEAVE_APP_RUN_ID, runId.getId())
         .put(EnvKeys.WEAVE_ZK_CONNECT, zkClient.getConnectString())
         .put(EnvKeys.WEAVE_LOG_KAFKA_ZK, getKafkaZKConnect())
         .build()
        );

      LOG.info("Starting runnable " + runnableName + " in container " + container);

      WeaveContainerLauncher launcher = new WeaveContainerLauncher(weaveSpec.getRunnables().get(runnableName),
                                                                   containerRunId, processLauncher,
                                                                   ZKClients.namespace(zkClient,
                                                                                       getZKNamespace(runnableName)),
                                                                   runnableArgs.get(runnableName),
                                                                   instanceId, instanceCounts.get(runnableName));
      runningContainers.add(runnableName, container.getId(),
                            launcher.start(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
                                           ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

      // Seems like a bug in amrm causing manual removal is needed.
      amrmClient.removeContainerRequest(provisionRequest.getRequest());

      if (runningContainers.count(runnableName) == containerCount) {
        LOG.info("Runnable " + runnableName + " fully provisioned with " + containerCount + " instances.");
        provisioning.poll();
      }
    }
  }

  private List<LocalFile> getLocalFiles() {
    try {
      Reader reader = Files.newReader(new File("localFiles.json"), Charsets.UTF_8);
      try {
        return new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
                                .create().fromJson(reader, new TypeToken<List<LocalFile>>() {}.getType());
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String getZKNamespace(String runnableName) {
    return String.format("/%s/runnables/%s", runId.getId(), runnableName);
  }

  private String getKafkaZKConnect() {
    return String.format("%s/%s/kafka", zkClient.getConnectString(), runId.getId());
  }

  private Properties generateKafkaConfig() {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("log.dir", new File("kafka-logs").getAbsolutePath());
    prop.setProperty("zk.connect", getKafkaZKConnect());
    prop.setProperty("num.threads", "8");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("log.flush.interval", "10000");
    prop.setProperty("max.socket.request.bytes", "104857600");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("log.default.flush.scheduler.interval.ms", "1000");
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("socket.receive.buffer", "1048576");
    prop.setProperty("enable.zookeeper", "true");
    prop.setProperty("log.retention.hours", "168");
    prop.setProperty("brokerid", "0");
    prop.setProperty("socket.send.buffer", "1048576");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.file.size", "536870912");
    prop.setProperty("log.default.flush.interval.ms", "1000");
    return prop;
  }

  private ListenableFuture<String> processMessage(final String messageId, Message message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Message received: " + messageId + " " + message);
    }

    SettableFuture<String> result = SettableFuture.create();
    Runnable completion = getMessageCompletion(messageId, result);
    if (handleSetInstances(message, completion)) {
      return result;
    }

    // Replicate messages to all runnables
    if (message.getScope() == Message.Scope.ALL_RUNNABLE) {
      runningContainers.sendToAll(message, completion);
      return result;
    }

    // Replicate message to a particular runnable.
    if (message.getScope() == Message.Scope.RUNNABLE) {
      runningContainers.sendToRunnable(message.getRunnableName(), message, completion);
      return result;
    }

    LOG.info("Message ignored. {}", message);
    return Futures.immediateFuture(messageId);
  }

  /**
   * Attempts to change the number of running instances.
   * @return {@code true} if the message does requests for changes in number of running instances of a runnable,
   *         {@code false} otherwise.
   */
  private boolean handleSetInstances(final Message message, final Runnable completion) {
    if (message.getType() != Message.Type.SYSTEM || message.getScope() != Message.Scope.RUNNABLE) {
      return false;
    }

    Command command = message.getCommand();
    Map<String, String> options = command.getOptions();
    if (!"instances".equals(command.getCommand()) || !options.containsKey("count")) {
      return false;
    }

    final String runnableName = message.getRunnableName();
    if (runnableName == null || runnableName.isEmpty() || !weaveSpec.getRunnables().containsKey(runnableName)) {
      LOG.info("Unknown runnable {}", runnableName);
      return false;
    }

    final int newCount = Integer.parseInt(options.get("count"));
    final int oldCount = instanceCounts.get(runnableName);

    if (newCount == oldCount) {   // Nothing to do
      return true;
    }

    LOG.info("Received change instances request. From {} to {}.", oldCount, newCount);

    instanceChangeExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          // Wait until running container count is the same as old count
          while (isRunning() && oldCount != runningContainers.count(runnableName)) {
            runningContainers.waitForChange();
          }
          instanceCounts.put(runnableName, newCount);

          try {
            if (newCount < oldCount) {
              // Shutdown some running containers
              for (int i = 0; i < oldCount - newCount; i++) {
                runningContainers.removeLast(runnableName);
              }
            } else {
              WeaveSpecification.Order order = Iterables.find(weaveSpec.getOrders(),
                                                              new Predicate<WeaveSpecification.Order>() {
                @Override
                public boolean apply(WeaveSpecification.Order input) {
                  return (input.getNames().contains(runnableName));
                }
              });

              RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);
              Resource capability = createCapability(runtimeSpec.getResourceSpecification());
              runnableContainerRequests.add(
                new RunnableContainerRequest(order.getType(), ImmutableMultimap.of(capability, runtimeSpec)));
            }
          } finally {
            LOG.info("Change instances completed. From {} to {}.", oldCount, newCount);
            runningContainers.sendToRunnable(runnableName, message, completion);
          }
        } catch (InterruptedException e) {
          // If the wait is being interrupted, discard the message.
          completion.run();
        }
      }
    });
    return true;
  }

  private Runnable getMessageCompletion(final String messageId, final SettableFuture<String> future) {
    return new Runnable() {
      @Override
      public void run() {
        future.set(messageId);
      }
    };
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    int cores = resourceSpec.getCores();
    // The following hack is to workaround the still unstable Resource interface.
    // With the latest YARN API, it should be like this:
//    int cores = Math.max(Math.min(resourceSpec.getCores(), maxCapability.getVirtualCores()),
//                         minCapability.getVirtualCores());
//    capability.setVirtualCores(cores);
    try {
      Method getVirtualCores = Resource.class.getMethod("getVirtualCores");
      Method setVirtualCores = Resource.class.getMethod("setVirtualCores", int.class);

      cores = Math.max(Math.min(cores, (Integer) getVirtualCores.invoke(maxCapability)),
                       (Integer) getVirtualCores.invoke(minCapability));
      setVirtualCores.invoke(capability, cores);
    } catch (Exception e) {
      // It's ok to ignore this exception, as it's using older version of API.
      LOG.info("Virtual cores limit not supported.");
    }

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

  /**
   * A private class for service lifecycle. It's done this way so that we can have {@link ZKServiceDecorator} to
   * wrap around this to reflect status in ZK.
   */
  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

    @Override
    protected void run() throws Exception {
      runThread = Thread.currentThread();
      try {
        doRun();
      } catch (InterruptedException e) {
        // It's ok to get interrupted exception, as it's a signal to stop
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
    public ListenableFuture<String> onReceived(String messageId, Message message) {
      return processMessage(messageId, message);
    }
  }
}
