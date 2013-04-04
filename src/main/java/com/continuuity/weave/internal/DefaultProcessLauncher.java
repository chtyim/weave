package com.continuuity.weave.internal;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class DefaultProcessLauncher implements ProcessLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProcessLauncher.class);

  private final Container container;
  private final YarnRPC yarnRPC;
  private final YarnConfiguration yarnConf;

  DefaultProcessLauncher(Container container, YarnRPC yarnRPC, YarnConfiguration yarnConf) {
    this.container = container;
    this.yarnRPC = yarnRPC;
    this.yarnConf = yarnConf;
  }

  @Override
  public PrepareLaunchContext prepareLaunch() {
    return new PrepareLaunchContextImpl();
  }

  /**
   * Helper to connect to container manager (node manager)
   */
  private ContainerManager connectContainerManager() {
    String cmIpPortStr = String.format("%s:%d", container.getNodeId().getHost(), container.getNodeId().getPort());
    InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
    return ((ContainerManager) yarnRPC.getProxy(ContainerManager.class, cmAddress, yarnConf));
  }


  private final class PrepareLaunchContextImpl implements PrepareLaunchContext {

    private final ContainerLaunchContext launchContext;
    private final Map<String, LocalResource> localResources;
    private final List<String> commands;

    PrepareLaunchContextImpl() {
      this.launchContext = Records.newRecord(ContainerLaunchContext.class);
      launchContext.setContainerId(container.getId());
      launchContext.setResource(container.getResource());

      this.localResources = Maps.newHashMap();
      this.commands = Lists.newLinkedList();
    }

    @Override
    public AfterUser setUser(String user) {
      launchContext.setUser(user);
      return new AfterUserImpl();
    }

    private final class AfterUserImpl implements PrepareLaunchContext.AfterUser {

      @Override
      public PrepareLaunchContext.ResourcesAdder withResources() {
        return new MoreResourcesImpl();
      }

      @Override
      public PrepareLaunchContext.AfterResources noResources() {
        return new MoreResourcesImpl();
      }
    }

    private final class MoreResourcesImpl implements PrepareLaunchContext.MoreResources {

      @Override
      public PrepareLaunchContext.MoreResources add(String name, LocalResource resource) {
        localResources.put(name, resource);
        return this;
      }

      @Override
      public PrepareLaunchContext.CommandAdder withCommands() {
        launchContext.setLocalResources(localResources);
        return new MoreCommandImpl();
      }
    }

    private final class MoreCommandImpl implements PrepareLaunchContext.MoreCommand,
                                                   PrepareLaunchContext.StdOutSetter,
                                                   PrepareLaunchContext.StdErrSetter {

      private final StringBuilder commandBuilder = new StringBuilder();

      @Override
      public PrepareLaunchContext.StdOutSetter add(String cmd, String... args) {
        Joiner.on(' ').appendTo(commandBuilder, cmd, "", args);
        return this;
      }

      @Override
      public ProcessController launch() {
        LOG.info("Launching in container " + container.getId() + ": " + commands);
        launchContext.setCommands(commands);

        // TODO: Hack now
        launchContext.setEnvironment(ImmutableMap.of("CLASSPATH", System.getProperty("java.class.path"),
                                                     "CONTAINER_HOST", container.getNodeId().getHost(),
                                                     "CONTAINER_PORT", Integer.toString(container.getNodeId().getPort()
        )));

        StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
        startRequest.setContainerLaunchContext(launchContext);
        ContainerManager containerManager = connectContainerManager();
        try {
          containerManager.startContainer(startRequest);
          return new ProcessControllerImpl(containerManager);
        } catch (YarnRemoteException e) {
          LOG.error("Error in launching process", e);
          throw Throwables.propagate(e);
        }
      }

      @Override
      public PrepareLaunchContext.MoreCommand redirectError(String stderr) {
        commandBuilder.append(' ').append("2>").append(stderr);
        return noError();
      }

      @Override
      public PrepareLaunchContext.MoreCommand noError() {
        commands.add(commandBuilder.toString());
        commandBuilder.setLength(0);
        return this;
      }

      @Override
      public PrepareLaunchContext.StdErrSetter redirectOutput(String stdout) {
        commandBuilder.append(' ').append("1>").append(stdout);
        return this;
      }

      @Override
      public PrepareLaunchContext.StdErrSetter noOutput() {
        return this;
      }
    }
  }

  private final class ProcessControllerImpl implements ProcessController {

    private final ContainerManager containerManager;

    private ProcessControllerImpl(ContainerManager containerManager) {
      this.containerManager = containerManager;
    }

    @Override
    public void stop() {
      LOG.info("Request to stop container: " + container.getId());
      StopContainerRequest stopRequest = Records.newRecord(StopContainerRequest.class);
      stopRequest.setContainerId(container.getId());
      try {
        containerManager.stopContainer(stopRequest);
        boolean completed = false;
        while (!completed) {
          GetContainerStatusRequest statusRequest = Records.newRecord(GetContainerStatusRequest.class);
          statusRequest.setContainerId(container.getId());
          GetContainerStatusResponse statusResponse = containerManager.getContainerStatus(statusRequest);
          LOG.info("Container status: " + statusResponse.getStatus() + " " + statusResponse.getStatus().getDiagnostics());

          completed = (statusResponse.getStatus().getState() == ContainerState.COMPLETE);
        }
      } catch (YarnRemoteException e) {
        LOG.error("Fail to stop container.", e);
      }
    }

    @Override
    public void status() {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }
}
