/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.zookeeper;

import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 *
 */
public class ZKOperationsTest {

  @Test
  public void recursiveDelete() throws ExecutionException, InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setTickTime(1000).build();
    zkServer.startAndWait();

    try {
      ZKClientService client = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      client.startAndWait();

      try {
        client.create("/test1/test10/test101", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test10/test102", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test10/test103", null, CreateMode.PERSISTENT).get();

        client.create("/test1/test11/test111", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test11/test112", null, CreateMode.PERSISTENT).get();
        client.create("/test1/test11/test113", null, CreateMode.PERSISTENT).get();

        ZKOperations.recursiveDelete(client, "/test1").get();


      } finally {
        client.stopAndWait();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }
}
