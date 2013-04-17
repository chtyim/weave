/**
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *   use this file except in compliance with the License. You may obtain a copy of
 *   the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations under
 *   the License.
 */

package com.continuuity.internal.discovery;

import com.continuuity.discovery.Cancellable;
import com.continuuity.discovery.Discoverable;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Singleton;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Zookeeper implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 */
@Singleton
public class ZookeeperDiscoveryService extends AbstractIdleService
  implements DiscoveryService, DiscoveryServiceClient {

  private Multimap<String, Discoverable> services;
  private final Lock lock = new ReentrantLock();

  @Override
  protected void startUp() throws Exception {
    services = HashMultimap.create();
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  @Override
  public Cancellable register(final Discoverable discoverable) {
    Preconditions.checkState(isRunning(), "Service is not running");
    lock.lock();
    try {
      final Discoverable wrapper = new DiscoverableWrapper(discoverable);
      services.put(wrapper.getName(), wrapper);
      return new Cancellable() {
        @Override
        public void cancel() {
          lock.lock();
          try {
            services.remove(wrapper.getName(), wrapper);
          } finally {
            lock.unlock();
          }
        }
      };
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Iterable<Discoverable> discover(final String name) {
    Preconditions.checkState(isRunning(), "Service is not running");
    return new Iterable<Discoverable>() {
      @Override
      public Iterator<Discoverable> iterator() {
        lock.lock();
        try {
          Preconditions.checkState(isRunning(), "Service is not running");
          return ImmutableList.copyOf(services.get(name)).iterator();
        } finally {
          lock.unlock();
        }
      }
    };
  }

  private static final class DiscoverableWrapper implements Discoverable {

    private final String name;
    private final InetSocketAddress address;

    private DiscoverableWrapper(Discoverable discoverable) {
      this.name = discoverable.getName();
      this.address = discoverable.getSocketAddress();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
      return address;
    }
  }
}

