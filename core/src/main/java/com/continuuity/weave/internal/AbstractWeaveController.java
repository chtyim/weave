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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogEntry;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.internal.json.StackTraceElementCodec;
import com.continuuity.weave.internal.kafka.client.SimpleKafkaClient;
import com.continuuity.weave.internal.logging.LogEntryDecoder;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.weave.kafka.client.FetchedMessage;
import com.continuuity.weave.kafka.client.KafkaClient;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A abstract base class for {@link WeaveController} implementation that uses Zookeeper to controller a
 * running weave application.
 */
public abstract class AbstractWeaveController extends AbstractZKServiceController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWeaveController.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 2000;

  private final Queue<LogHandler> logHandlers;
  private final KafkaClient kafkaClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Thread logPoller;

  public AbstractWeaveController(RunId runId, ZKClient zkClient, Iterable<LogHandler> logHandlers) {
    super(runId, zkClient);
    this.logHandlers = new ConcurrentLinkedQueue<LogHandler>();
    this.kafkaClient = new SimpleKafkaClient(ZKClients.namespace(zkClient, "/" + runId.getId() + "/kafka"));
    this.discoveryServiceClient = new ZKDiscoveryService(zkClient);
    this.logPoller = createLogPoller();
    Iterables.addAll(this.logHandlers, logHandlers);
  }

  @Override
  protected void doStartUp() {
    if (!logHandlers.isEmpty()) {
      logPoller.start();
    }
  }

  @Override
  protected void shutDown() {
    logPoller.interrupt();
    try {
      // Wait for the poller thread to stop.
      logPoller.join(SHUTDOWN_TIMEOUT_MS);
    } catch (InterruptedException e) {
      LOG.warn("Joining of log poller thread interrupted.", e);
    }
  }

  @Override
  public final synchronized void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    if (!logPoller.isAlive()) {
      logPoller.start();
    }
  }

  @Override
  public Iterable<Discoverable> discoverService(String serviceName) {
    return discoveryServiceClient.discover(serviceName);
  }

  @Override
  public void changeInstances(String runnable, int newCount) {
    try {
//      sendMessage(SystemMessages.setInstances(runnable, newCount), runnable).get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void doCancel() {
    logPoller.interrupt();
    try {
      logPoller.join();
    } catch (InterruptedException e) {
      LOG.warn("Joining of log poller thread interrupted.", e);
    }
  }

  private Thread createLogPoller() {
    Thread poller = new Thread("weave-log-poller") {
      @Override
      public void run() {
        LOG.info("Weave log poller thread started.");
        kafkaClient.startAndWait();
        Gson gson = new GsonBuilder().registerTypeAdapter(LogEntry.class, new LogEntryDecoder())
                                     .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
                                     .create();
        Iterator<FetchedMessage> messageIterator = kafkaClient.consume(Constants.LOG_TOPIC, 0, 0, 1048576);
        while (messageIterator.hasNext()) {
          String json = Charsets.UTF_8.decode(messageIterator.next().getBuffer()).toString();
          try {
            LogEntry entry = gson.fromJson(json, LogEntry.class);
            if (entry != null) {
              invokeHandlers(entry);
            }
          } catch (Exception e) {
            LOG.error("Failed to decode log entry {}", json, e);
          }
        }
        kafkaClient.stopAndWait();
        LOG.info("Weave log poller thread stopped.");
      }

      private void invokeHandlers(LogEntry entry) {
        for (LogHandler handler : logHandlers) {
          handler.onLog(entry);
        }
      }
    };
    poller.setDaemon(true);
    return poller;
  }
}
