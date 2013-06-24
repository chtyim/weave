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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ControllerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ControllerTest.class);

  @Test
  public void testController() throws ExecutionException, InterruptedException, TimeoutException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());

    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      Service service = createService(zkClientService, runId);
      service.startAndWait();

      WeaveController controller = getController(zkClientService, runId);

      controller.sendCommand(Command.Builder.of("test").build()).get(2, TimeUnit.SECONDS);
      controller.stop().get(2, TimeUnit.SECONDS);

      Assert.assertEquals(ServiceController.State.TERMINATED, controller.getState());

      final CountDownLatch terminateLatch = new CountDownLatch(1);
      service.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          terminateLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Assert.assertTrue(service.state() == Service.State.TERMINATED || terminateLatch.await(2, TimeUnit.SECONDS));

      zkClientService.stopAndWait();

    } finally {
      zkServer.stopAndWait();
    }
  }

  // Test controller created before service starts.
  @Test
  public void testControllerBefore() throws InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());
    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      final CountDownLatch runLatch = new CountDownLatch(1);
      final CountDownLatch stopLatch = new CountDownLatch(1);
      WeaveController controller = getController(zkClientService, runId);
      controller.addListener(new ListenerAdapter() {
        @Override
        public void running() {
          runLatch.countDown();
        }

        @Override
        public void terminated() {
          stopLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Service service = createService(zkClientService, runId);
      service.start();

      Assert.assertTrue(runLatch.await(2, TimeUnit.SECONDS));
      Assert.assertFalse(stopLatch.await(2, TimeUnit.SECONDS));

      service.stop();

      Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));

    } finally {
      zkServer.stopAndWait();
    }
  }

  // Test controller listener receive first state change without state transition from service
  @Test
  public void testControllerListener() throws InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());
    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      Service service = createService(zkClientService, runId);
      service.startAndWait();

      final CountDownLatch runLatch = new CountDownLatch(1);
      WeaveController controller = getController(zkClientService, runId);
      controller.addListener(new ListenerAdapter() {
        @Override
        public void running() {
          runLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Assert.assertTrue(runLatch.await(2, TimeUnit.SECONDS));

      service.stopAndWait();

      zkClientService.stopAndWait();
    } finally {
      zkServer.stopAndWait();
    }
  }

  private Service createService(ZKClient zkClient, RunId runId) {
    return new ZKServiceDecorator(
      zkClient, runId, Suppliers.ofInstance(new JsonObject()), new AbstractIdleService() {

      @Override
      protected void startUp() throws Exception {
        LOG.info("Start");
      }

      @Override
      protected void shutDown() throws Exception {
        LOG.info("Stop");
      }
    });
  }

  private WeaveController getController(ZKClient zkClient, RunId runId) {
    AbstractWeaveController controller = new AbstractWeaveController(zkClient, runId, ImmutableList.<LogHandler>of()) {

      @Override
      public void kill() {
        // No-op
      }
    };
    controller.start();
    return controller;
  }
}
