package com.continuuity.weave.zk;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for killing ZK client to similate failures during testing.
 */
public final class KillZKSession {

  /**
   * Utility classes should have a public constructor or a default constructor
   * hence made it private.
   */
  private KillZKSession() {}

  /**
   * Kills a Zookeeper client to simulate failure scenarious during testing.
   * Callee will provide the amount of time to wait before it's considered failure
   * to kill a client.
   *
   * @param client that needs to be killed.
   * @param connectionString of Quorum
   * @param maxMs time in millisecond specifying the max time to kill a client.
   * @throws Exception
   */
  public static void kill(ZooKeeper client, String connectionString,
                          int maxMs) throws IOException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zk = new ZooKeeper(connectionString, maxMs, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
    }, client.getSessionId(), client.getSessionPasswd());

    try {
      Preconditions.checkState(latch.await(maxMs, TimeUnit.MILLISECONDS), "Fail to kill ZK connection.");
    } finally {
      zk.close();
    }
  }
}