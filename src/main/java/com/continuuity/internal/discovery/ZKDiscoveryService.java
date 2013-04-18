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

import com.continuuity.weave.zookeeper.Cancellable;
import com.continuuity.weave.zookeeper.Discoverable;
import com.continuuity.weave.zookeeper.DiscoveryService;
import com.continuuity.weave.zookeeper.DiscoveryServiceClient;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Singleton;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.File;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zookeeper implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 * <p>
 *   Discoverable services are registered within Zookeeper under the namespace 'discoverable' by default.
 *   If you would like to change the namespace under which the services are registered then you can pass
 *   in the namespace during construction of {@link ZKDiscoveryService}.
 * </p>
 *
 * <p>
 *   Following is a simple example of how {@link ZKDiscoveryService} can be used for registering services
 *   and also for discovering the registered services.
 *   <blockquote>
 *    <pre>
 *      DiscoveryService service = new ZKDiscoveryService(zkQuorum);
 *      service.startAndWait();
 *      service.register(new Discoverable() {
 *        @Override
 *        public String getName() {
 *          return 'service-name';
 *        }
 *
 *        @Override
 *        public InetSocketAddress getSocketAddress() {
 *          return new InetSocketAddress(hostname, port);
 *        }
 *      });
 *      ...
 *      ...
 *      Iterable<Discoverable> services = service.discovery("service-name");
 *      ...
 *    </pre>
 *   </blockquote>
 * </p>
 */
@Singleton
public class ZKDiscoveryService extends AbstractIdleService implements DiscoveryService, DiscoveryServiceClient {
  private static final String NAMESPACE = "/discoverable";
  private final AtomicReference<Multimap<String, Discoverable>> services;
  private final ZKClientService client;
  private final String namespace;

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper quorum for storing service registry.
   * @param zkConnectionString of zookeeper quorum
   */
  public ZKDiscoveryService(String zkConnectionString) {
    this(zkConnectionString, NAMESPACE);
  }

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper quorum for storing service registry under namepsace.
   * @param zkConnectionString of zookeeper quorum
   * @param namespace under which the service registered would be stored in zookeeper.
   */
  public ZKDiscoveryService(String zkConnectionString, String namespace) {
    client = ZKClientService.Builder.of(zkConnectionString).build();
    this.namespace = namespace;
    services = new AtomicReference<Multimap<String, Discoverable>>();
  }

  @Override
  protected void startUp() throws Exception {
    client.startAndWait();
    Multimap<String, Discoverable> mappings = HashMultimap.create();
    services.set(mappings);
  }

  @Override
  protected void shutDown() throws Exception {
    client.stopAndWait();
  }

  /**
   * Registers a {@link Discoverable} in zookeeper.
   * <p>
   *   Registering a {@link Discoverable} will create a node <base>/<service-name>
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with emphemeral, then a runtime
   *   exception is thrown to make sure that a service with an intent to register is not started without registering.
   *   When a runtime is thrown, expectation is that the process being started with fail and would be started again
   *   by the monitoring service.
   * </p>
   * @param discoverable Information of the service provider that could be discovered.
   * @return An instance of {@link Cancellable}
   */
  @Override
  public Cancellable register(final Discoverable discoverable) {
    Preconditions.checkState(isRunning(), "Service is not running");

    final Discoverable wrapper = new DiscoverableWrapper(discoverable);
    byte[] discoverableBytes = encode(wrapper);

    // Path <discoverable-base>/<service-name>
    final String sb = namespace + "/" + wrapper.getName() + "/service-";

    try {
      final String path = client.create(sb, discoverableBytes, CreateMode.EPHEMERAL_SEQUENTIAL, true).get();
      return new Cancellable() {
        @Override
        public void cancel() {
          try {
            if(client.exists(path).get() != null) {
              client.delete(path).get();
            }
          } catch(Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets the list of {@link Discoverable} for a given service.
   * <p>
   *   Once, the list is retrieved a watcher is set on the service to track
   *   any changes that would require us to reload the service information
   *   information from zookeeper.
   * </p>
   * @param service for which we are requested to retrieve the list of {@link Discoverable}
   */
  private void getChildren(final String service) {
    final String sb = namespace + "/" + service;
    try {
      if(! client.isRunning()) {
        return;
      }
      List<String> children = client.getChildren(sb, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if(event.getType() == Event.EventType.NodeChildrenChanged) {
            try {
              getChildren(new File(event.getPath()).getName());
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
      }).get().getChildren();

      // Make a copy of services, remove the service that has changed and then
      // add the new endpoints available in zookeeper.
      Multimap<String, Discoverable> newServices = HashMultimap.create(services.get());
      newServices.removeAll(service);
      for(String child : children) {
        String path = sb + "/" + child;
        if(client.exists(path).get() != null) {
          NodeData data = client.getData(path).get();
          newServices.put(service, decode(data.getData()));
        }
      }
      // Replace the local service register with changes.
      services.set(newServices);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Discovers a <code>service</code> available
   *
   * @param service name of the service to be discovered.
   * @return Live {@link Iterable} of {@link Discoverable} <code>service</code>
   */
  @Override
  public Iterable<Discoverable> discover(final String service) {
    Preconditions.checkState(isRunning(), "Service is not running");
    return new Iterable<Discoverable>() {
      @Override
      public Iterator<Discoverable> iterator() {
        Preconditions.checkState(isRunning(), "Service is not running");
        if(! services.get().containsKey(service)) {
          getChildren(service);
        }
        return ImmutableList.copyOf(services.get().get(service)).iterator();
      }
    };
  }

  /**
   * Static helper function for decoding array of bytes into a {@link DiscoverableWrapper} object.
   * @param bytes representing serialized {@link DiscoverableWrapper}
   * @return null if bytes are null; else an instance of {@link DiscoverableWrapper}
   */
  static DiscoverableWrapper decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String content = new String(bytes, Charsets.UTF_8);
    return new GsonBuilder().registerTypeAdapter(DiscoverableWrapper.class, new DiscoverableCodec())
      .create()
      .fromJson(content, DiscoverableWrapper.class);
  }

  /**
   * Static helper function for encoding an instance of {@link DiscoverableWrapper} into array of bytes.
   * @param discoverable An instance of {@link DiscoverableWrapper}
   * @return array of bytes representing an instance of <code>discoverable</code>
   */
  static byte[] encode(Discoverable discoverable) {
    return new GsonBuilder().registerTypeAdapter(DiscoverableWrapper.class, new DiscoverableCodec())
      .create()
      .toJson(discoverable, DiscoverableWrapper.class)
      .getBytes(Charsets.UTF_8);
  }

  /**
   * SerDe for converting a {@link DiscoverableWrapper} into a JSON object
   * or from a JSON object into {@link DiscoverableWrapper}.
   */
  private static final class DiscoverableCodec
    implements JsonSerializer<DiscoverableWrapper>, JsonDeserializer<DiscoverableWrapper> {

    @Override
    public DiscoverableWrapper deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      final String service = jsonObj.get("service").getAsString();
      final String hostname = jsonObj.get("hostname").getAsString();
      final int port = jsonObj.get("port").getAsInt();
      return new DiscoverableWrapper(
        new Discoverable() {
          @Override
          public String getName() {
            return service;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return new InetSocketAddress(hostname, port);
          }
        }
      );
    }

    @Override
    public JsonElement serialize(DiscoverableWrapper src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("service", src.getName());
      jsonObj.addProperty("hostname", src.getSocketAddress().getHostName());
      jsonObj.addProperty("port", src.getSocketAddress().getPort());
      return jsonObj;
    }
  }

  /**
   * Wrapper for a discoverable.
   */
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

