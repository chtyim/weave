package com.continuuity.weave.state;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ZKServiceDecoratorTest {

  private static final Logger LOG = LoggerFactory.getLogger(ZKServiceDecoratorTest.class);

  @Test
  public void testStateTransition() throws InterruptedException, ExecutionException, TimeoutException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    try {
      final String namespace = Joiner.on('/').join("/weave", RunIds.generate(), "runnables", "Runner1");

      final ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClient.startAndWait();
      zkClient.create(namespace, null, CreateMode.PERSISTENT).get();

      try {
        JsonObject content = new JsonObject();
        content.addProperty("containerId", "container-123");
        content.addProperty("host", "localhost");

        RunId runId = RunIds.generate();
        final Semaphore semaphore = new Semaphore(0);
        ZKServiceDecorator service = new ZKServiceDecorator(zkServer.getConnectionStr() + namespace, 10000,
                                                            runId, Suppliers.ofInstance(content),
                                                            new AbstractIdleService() {
          @Override
          protected void startUp() throws Exception {
            Preconditions.checkArgument(semaphore.tryAcquire(5, TimeUnit.SECONDS), "Fail to start");
          }

          @Override
          protected void shutDown() throws Exception {
            Preconditions.checkArgument(semaphore.tryAcquire(5, TimeUnit.SECONDS), "Fail to stop");
          }
        });

        final String runnablePath = namespace + "/" + runId;
        final AtomicReference<String> stateMatch = new AtomicReference<String>("STARTING");
        watchDataChange(zkClient, runnablePath + "/state", semaphore, stateMatch);
        Assert.assertEquals(Service.State.RUNNING, service.start().get(5, TimeUnit.SECONDS));

        stateMatch.set("STOPPING");
        Assert.assertEquals(Service.State.TERMINATED, service.stop().get(5, TimeUnit.SECONDS));

      } finally {
        zkClient.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  private void watchDataChange(final ZKClientService zkClient, final String path,
                               final Semaphore semaphore, final AtomicReference<String> stateMatch) {
    Futures.addCallback(zkClient.getData(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
          watchDataChange(zkClient, path, semaphore, stateMatch);
        }
      }
    }), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        String content = new String(result.getData(), Charsets.UTF_8);
        JsonObject json = new Gson().fromJson(content, JsonElement.class).getAsJsonObject();
        if (stateMatch.get().equals(json.get("state").getAsString())) {
          semaphore.release();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        exists();
      }

      private void exists() {
        Futures.addCallback(zkClient.exists(path, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeCreated) {
              watchDataChange(zkClient, path, semaphore, stateMatch);
            }
          }
        }), new FutureCallback<Stat>() {
          @Override
          public void onSuccess(Stat result) {
            if (result != null) {
              watchDataChange(zkClient, path, semaphore, stateMatch);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.error(t.getMessage(), t);
          }
        });
      }
    });
  }
}
