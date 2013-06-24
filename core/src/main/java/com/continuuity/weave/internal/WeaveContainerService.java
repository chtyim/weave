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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.utils.Instances;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class act as a yarn container and run a {@link WeaveRunnable}.
 */
public final class WeaveContainerService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainerService.class);

  private final WeaveRunnableSpecification specification;
  private final ClassLoader classLoader;
  private final ContainerInfo containerInfo;
  private final BasicWeaveContext context;
  private final ZKServiceDecorator serviceDelegate;
  private ExecutorService commandExecutor;
  private WeaveRunnable runnable;

  public WeaveContainerService(BasicWeaveContext context, ContainerInfo containerInfo, ZKClient zkClient,
                               RunId runId, WeaveRunnableSpecification specification, ClassLoader classLoader) {
    this.specification = specification;
    this.classLoader = classLoader;
    this.serviceDelegate = new ZKServiceDecorator(zkClient, runId, createLiveNodeSupplier(), new ServiceDelegate());
    this.context = context;
    this.containerInfo = containerInfo;
  }

  private ListenableFuture<String> processMessage(final String messageId, final Message message) {
    LOG.info("Message received: " + message);
    final SettableFuture<String> result = SettableFuture.create();
    Command command = message.getCommand();
    if (message.getType() == Message.Type.SYSTEM
          && "instances".equals(command.getCommand()) && command.getOptions().containsKey("count")) {
      context.setInstanceCount(Integer.parseInt(command.getOptions().get("count")));
    }

    commandExecutor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          runnable.handleCommand(message.getCommand());
          result.set(messageId);
        } catch (Exception e) {
          result.setException(e);
        }
      }
    });
    return result;
  }

  private Supplier<? extends JsonElement> createLiveNodeSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("containerId", containerInfo.getId());
        jsonObj.addProperty("host", containerInfo.getHost().getCanonicalHostName());
        return jsonObj;
      }
    };
  }

  @Override
  public ListenableFuture<State> start() {
    commandExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("runnable-command-executor"));
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
    commandExecutor.shutdownNow();
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

    @Override
    protected void startUp() throws Exception {
      Class<?> runnableClass = classLoader.loadClass(specification.getClassName());
      Preconditions.checkArgument(WeaveRunnable.class.isAssignableFrom(runnableClass),
                                  "Class %s is not instance of WeaveRunnable.", specification.getClassName());

      runnable = Instances.newInstance((Class<WeaveRunnable>) runnableClass);
      runnable.initialize(context);
    }

    @Override
    protected void triggerShutdown() {
      try {
        runnable.stop();
      } catch (Throwable t) {
        LOG.error("Exception when stopping runnable.", t);
      }
    }

    @Override
    protected void run() throws Exception {
      runnable.run();
    }

    @Override
    public ListenableFuture<String> onReceived(String messageId, Message message) {
      return processMessage(messageId, message);
    }
  }
}
