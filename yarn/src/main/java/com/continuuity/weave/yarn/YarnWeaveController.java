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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.AbstractWeaveController;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class YarnWeaveController extends AbstractWeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(YarnWeaveController.class);

  /**
   * Max time to wait for application status from yarn when stopping an application.
   */
  private static final long MAX_STOP_TIME = 5000;   // 5 seconds

  private final YarnClient yarnClient;
  private volatile ApplicationId applicationId;

  YarnWeaveController(YarnClient yarnClient, ZKClient zkClient,
                      ApplicationId appId, RunId runId, Iterable<LogHandler> logHandlers) {
    super(runId, zkClient, logHandlers);
    this.yarnClient = yarnClient;
    this.applicationId = appId;
  }

//  @Override
//  public void start() {
//    super.start();
//
//    // Add a listener for setting application Id
//    addListener(new ListenerAdapter() {
//      @Override
//      public void running() {
//        // Set only if it is not passed in.
//        if (applicationId != null) {
//          return;
//        }
//        applicationId = fetchApplicationId();
//      }
//    }, Threads.SAME_THREAD_EXECUTOR);
//  }
//
//  @Override
//  public ListenableFuture<State> stop() {
//    final ApplicationId appId = applicationId == null ? fetchApplicationId() : applicationId;
//
//    return Futures.transform(super.stop(), new Function<State, State>() {
//      @Override
//      public State apply(State state) {
//        if (appId == null) {
//          LOG.warn("ApplicationId unknown.");
//          return state;
//        }
//        try {
//          StopWatch stopWatch = new StopWatch();
//          stopWatch.start();
//          stopWatch.split();
//          // At most 5 seconds.
//          boolean done = false;
//          while (!done && stopWatch.getSplitTime() < MAX_STOP_TIME) {
//            LOG.info("Fetching application report for " + appId);
//            ApplicationReport report = yarnClient.getApplicationReport(appId);
//            YarnApplicationState appState = report.getYarnApplicationState();
//            switch (appState) {
//              case FINISHED:
//                LOG.info("Application finished.");
//                done = true;
//                break;
//              case FAILED:
//                LOG.warn("Application failed.");
//                done = true;
//                break;
//              case KILLED:
//                LOG.warn("Application killed.");
//                done = true;
//                break;
//            }
//            TimeUnit.SECONDS.sleep(1);
//            stopWatch.split();
//          }
//        } catch (Exception e) {
//          LOG.warn("Exception while waiting for application report: {}", e.getMessage(), e);
//        }
//        return state;
//      }
//    });
//  }

  @Override
  public void kill() {
    ApplicationId appId = applicationId == null ? fetchApplicationId() : applicationId;
    if (appId == null) {
      LOG.warn("ApplicationId unknown. Kill ignored.");
      return;
    }

    try {
      yarnClient.killApplication(appId);
    } catch (YarnRemoteException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Fetches applicationId from the ZK instance node.
   */
  private ApplicationId fetchApplicationId() {
//    byte[] data = getLiveNodeData();
//    if (data == null) {
//      LOG.warn("No live data node.");
//      return null;
//    }
//    JsonElement json = new Gson().fromJson(new String(data, Charsets.UTF_8), JsonElement.class);
//    if (!json.isJsonObject()) {
//      LOG.warn("Unable to decode live data node.");
//      return null;
//    }
//    JsonObject jsonObj = json.getAsJsonObject();
//    json = jsonObj.get("data");
//    if (!json.isJsonObject()) {
//      LOG.warn("Property data not found in live data node.");
//      return null;
//    }
//    jsonObj = json.getAsJsonObject();
//    ApplicationId appId = Records.newRecord(ApplicationId.class);
//    appId.setId(jsonObj.get("appId").getAsInt());
//    appId.setClusterTimestamp(jsonObj.get("appIdClusterTime").getAsLong());
//
//    return appId;
    return null;
  }

  @Override
  protected void instanceNodeUpdated(NodeData nodeData) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
