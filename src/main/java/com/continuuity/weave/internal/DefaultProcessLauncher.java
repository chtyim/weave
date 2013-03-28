package com.continuuity.weave.internal;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class DefaultProcessLauncher implements ProcessLauncher {

  private final Container container;
  private final YarnRPC yarnRPC;
  private final YarnConfiguration yarnConf;
  private final ContainerLaunchContext launchContext;
  private final Map<String, LocalResource> localResources;
  private final List<String> commands;
  private volatile ContainerManager containerManager;

  DefaultProcessLauncher(Container container, YarnRPC yarnRPC, YarnConfiguration yarnConf) {
    this.container = container;
    this.yarnRPC = yarnRPC;
    this.yarnConf = yarnConf;

    this.launchContext = Records.newRecord(ContainerLaunchContext.class);
    launchContext.setContainerId(container.getId());
    launchContext.setResource(container.getResource());

    this.localResources = Maps.newHashMap();
    this.commands = Lists.newLinkedList();
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

    @Override
    public AfterUser setUser(String user) {
      launchContext.setUser(user);
      return new AfterUserImpl();
    }
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
      System.out.println(commands);
      launchContext.setCommands(commands);
      StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
      startRequest.setContainerLaunchContext(launchContext);
      containerManager = connectContainerManager();
      try {
        containerManager.startContainer(startRequest);
        return new ProcessControllerImpl();
      } catch (YarnRemoteException e) {
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

  private final class ProcessControllerImpl implements ProcessController {

    @Override
    public void stop() {
      // TODO
    }

    @Override
    public void status() {
      // TODO
    }
  }
}
