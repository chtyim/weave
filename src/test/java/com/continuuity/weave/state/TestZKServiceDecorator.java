package com.continuuity.weave.state;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.google.common.base.Charsets;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.JsonObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestZKServiceDecorator {

  private static final Logger LOG = LoggerFactory.getLogger(TestZKServiceDecorator.class);

  @Test
  public void test() throws InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    try {
      JsonObject content = new JsonObject();
      content.addProperty("containerId", "container-123");

      RunId id = RunIds.generate();
      ZKServiceDecorator service = new ZKServiceDecorator(zkServer.getConnectionStr(), 10000, "Test", id,
                                                          Suppliers.ofInstance(content),
                                                          new AbstractIdleService() {
        @Override
        protected void startUp() throws Exception {
          LOG.info("Start");
        }

        @Override
        protected void shutDown() throws Exception {
          LOG.info("stop");
        }
      }, new MessageCallback() {
        @Override
        public void onReceived(String id, byte[] message) {
          LOG.info("Message received: " + id + " " + new String(message, Charsets.UTF_8));
        }
      });

      service.startAndWait();
      TimeUnit.SECONDS.sleep(1000);
      service.stopAndWait();

      TimeUnit.SECONDS.sleep(5);

    } finally {
      zkServer.stopAndWait();
    }
  }
}
