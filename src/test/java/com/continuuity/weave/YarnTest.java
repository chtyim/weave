package com.continuuity.weave;

import com.continuuity.weave.internal.yarn.ApplicationMasterMain;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class YarnTest {

  private static Logger LOG = LoggerFactory.getLogger(YarnTest.class);

  @Test
  public void test() throws InterruptedException, IOException {
    // Starts Zookeeper
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    try {
      YarnConfiguration config = new YarnConfiguration(new Configuration());

      // TODO: Hack
      config.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler");
      config.set("yarn.minicluster.fixed.ports", "true");

      MiniYARNCluster cluster = new MiniYARNCluster("test-cluster", 1, 1, 1);
      cluster.init(config);
      cluster.start();

      LOG.info("Started");

      YarnClient yarnClient = new YarnClientImpl();
      yarnClient.init(config);
      yarnClient.start();
      GetNewApplicationResponse response = yarnClient.getNewApplication();

      ApplicationSubmissionContext appSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
      appSubmissionContext.setApplicationId(response.getApplicationId());
      appSubmissionContext.setApplicationName("Test");

      ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
      containerLaunchContext.setCommands(
        ImmutableList.of("java",
                         ApplicationMasterMain.class.getName(),
                         zkServer.getConnectionStr(),
                         "1>/tmp/out", "2>/tmp/err"));
      containerLaunchContext.setEnvironment(ImmutableMap.of("CLASSPATH", System.getProperty("java.class.path")));

      // Kafka
//      Map<String, LocalResource> localResources = Maps.newHashMap();
//      LocalResource kafkaResource = Records.newRecord(LocalResource.class);
//      kafkaResource.setType(LocalResourceType.FILE);
//      kafkaResource.setVisibility(LocalResourceVisibility.APPLICATION);
//      File kafkaArchive = new File("/Users/terence/Works/kafka-0.7.2-incubating-src.jar");
//      kafkaResource.setResource(ConverterUtils.getYarnUrlFromURI(kafkaArchive.toURI()));
//      kafkaResource.setTimestamp(kafkaArchive.lastModified());
//      kafkaResource.setSize(kafkaArchive.length());
//      localResources.put("kafka", kafkaResource);
//      containerLaunchContext.setLocalResources(localResources);

      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(256);
      containerLaunchContext.setResource(capability);

      appSubmissionContext.setAMContainerSpec(containerLaunchContext);

      LOG.info("Submitted");
      ApplicationId applicationId = yarnClient.submitApplication(appSubmissionContext);

      System.out.println(applicationId);

      TimeUnit.SECONDS.sleep(1000);

      yarnClient.stop();
      cluster.stop();

    } finally {
      zkServer.stopAndWait();
    }
  }
}