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
package com.continuuity.internal.discovery;

import com.continuuity.discovery.Cancellable;
import com.continuuity.discovery.Discoverable;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ranges;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.ranges.Range;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ZKDiscoveryServiceTest {
  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void beforeClass() {
    zkServer = InMemoryZKServer.builder().setTickTime(100000).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    zkServer.stopAndWait();
  }

  private Cancellable register(DiscoveryService service, final String name, final String host, final int port) {
    return service.register(new Discoverable() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
      }
    });
  }

  @Test
  public void simpleDiscoverable() throws Exception {
    ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkServer.getConnectionStr());
    discoveryService.startAndWait();
    try {
      // Register one service running on one host:port
      Cancellable cancellable = register(discoveryService, "foo", "localhost", 8090);
      Iterable<Discoverable> discoverables = discoveryService.discover("foo");

      // Discover that registered host:port.
      Assert.assertTrue(Iterables.size(discoverables) == 1);

      // Remove the service
      cancellable.cancel();

      // There should be no service.
      discoverables = discoveryService.discover("foo");
      Assert.assertTrue(Iterables.size(discoverables) == 0);
    } finally {
      discoveryService.stopAndWait();
    }
  }

  @Test
  public void manySameDiscoverable() throws Exception {
    List<Cancellable> cancellables = Lists.newArrayList();
    ZKDiscoveryService service = new ZKDiscoveryService(zkServer.getConnectionStr());
    service.startAndWait();

    try {
      cancellables.add(register(service, "manyDiscoverable", "localhost", 1));
      cancellables.add(register(service, "manyDiscoverable", "localhost", 2));
      cancellables.add(register(service, "manyDiscoverable", "localhost", 3));
      cancellables.add(register(service, "manyDiscoverable", "localhost", 4));
      cancellables.add(register(service, "manyDiscoverable", "localhost", 5));

      Iterable<Discoverable> discoverables = service.discover("manyDiscoverable");
      Assert.assertTrue(Iterables.size(discoverables) == 5);

      for(int i = 5; i > 1; --i) {
        cancellables.get(5 - i).cancel();
        discoverables = service.discover("manyDiscoverable");
        Assert.assertTrue(Iterables.size(discoverables) == i-1);
      }
    } finally {
      service.stopAndWait();
    }
  }

  @Test
  public void multiServiceDiscoverable() throws Exception {
    List<Cancellable> cancellables = Lists.newArrayList();
    ZKDiscoveryService service = new ZKDiscoveryService(zkServer.getConnectionStr());
    service.startAndWait();

    try {
      cancellables.add(register(service, "service1", "localhost", 1));
      cancellables.add(register(service, "service1", "localhost", 2));
      cancellables.add(register(service, "service1", "localhost", 3));
      cancellables.add(register(service, "service1", "localhost", 4));
      cancellables.add(register(service, "service1", "localhost", 5));

      cancellables.add(register(service, "service2", "localhost", 1));
      cancellables.add(register(service, "service2", "localhost", 2));
      cancellables.add(register(service, "service2", "localhost", 3));

      cancellables.add(register(service, "service3", "localhost", 1));
      cancellables.add(register(service, "service3", "localhost", 2));

      Iterable<Discoverable> discoverables = service.discover("service1");
      Assert.assertTrue(Iterables.size(discoverables) == 5);

      discoverables = service.discover("service2");
      Assert.assertTrue(Iterables.size(discoverables) == 3);

      discoverables = service.discover("service3");
      Assert.assertTrue(Iterables.size(discoverables) == 2);

      cancellables.add(register(service, "service3", "localhost", 3));
      Assert.assertTrue(Iterables.size(discoverables) == 3);
    } finally {
      for(Cancellable cancellable : cancellables) {
        cancellable.cancel();
      }
      service.stopAndWait();
    }
  }


}
