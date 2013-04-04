package com.continuuity.weave.internal.kafka;

import com.continuuity.weave.internal.zk.NodeChildren;
import com.continuuity.weave.internal.zk.NodeData;
import com.continuuity.weave.internal.zk.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
*
*/
final class KafkaBrokerCache extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerCache.class);

  private static final String BROKERS_PATH = "/brokers";

  private final ZKClientService zkClient;
  private final Map<String, InetSocketAddress> brokers;
  private final Map<String, Set<BrokerInfo>> topicBrokers;
  private final Runnable invokeGetBrokers = new Runnable() {
    @Override
    public void run() {
      getBrokers();
    }
  };
  private final Runnable invokeGetTopics = new Runnable() {
    @Override
    public void run() {
      getTopics();
    }
  };

  KafkaBrokerCache(ZKClientService zkClient) {
    this.zkClient = zkClient;
    this.brokers = Maps.newConcurrentMap();
    this.topicBrokers = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    getBrokers();
    getTopics();
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  private void getBrokers() {
    final String idsPath = BROKERS_PATH + "/ids";

    Futures.addCallback(zkClient.getChildren(idsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        getBrokers();
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(idsPath, invokeGetBrokers) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());
        for (String child : children) {
          getBrokenData(idsPath + "/" + child, child);
        }
        // Remove all removed brokers
        removeDiff(children, brokers);
      }
    });
  }

  private void getTopics() {
    final String topicsPath = BROKERS_PATH + "/topics";
    Futures.addCallback(zkClient.getChildren(topicsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        getTopics();
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(topicsPath, invokeGetTopics) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());

        // Process new children
        for (String topic : ImmutableSet.copyOf(Sets.difference(children, topicBrokers.keySet()))) {
          getTopic(topicsPath + "/" + topic, topic);
        }

        // Remove old children
        removeDiff(children, topicBrokers);
      }
    });
  }

  private void getBrokenData(String path, final String brokerId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        String data = new String(result.getData(), Charsets.UTF_8);
        String hostPort = data.substring(data.indexOf(':') + 1);
        int idx = hostPort.indexOf(':');
        brokers.put(brokerId, new InetSocketAddress(hostPort.substring(0, idx),
                                                    Integer.parseInt(hostPort.substring(idx + 1))));
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op, the watch on the parent node will handle it.
      }
    });
  }

  private void getTopic(final String path, final String topic) {
    Futures.addCallback(zkClient.getChildren(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // Other event type changes are either could be ignored or handled by parent watcher
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
          getTopic(path, topic);
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        topicBrokers.put(topic, Sets.<BrokerInfo>newHashSet());
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op. Failure would be handled by parent watcher already (e.g. node not exists -> children change in parent)
      }
    });
  }

  private <K,V> void removeDiff(Set<K> keys, Map<K,V> map) {
    for (K key : ImmutableSet.copyOf(Sets.difference(map.keySet(), keys))) {
      map.remove(key);
    }
  }

  private abstract class ExistsOnFailureFutureCallback<V> implements FutureCallback<V> {

    private final String path;
    private final Runnable action;

    protected ExistsOnFailureFutureCallback(String path, Runnable action) {
      this.path = path;
      this.action = action;
    }

    @Override
    public final void onFailure(Throwable t) {
      if (isNotExists(t)) {
        LOG.error("Fail to watch for kafka brokers.", t);
        return;
      }

      waitExists(path);
    }

    private boolean isNotExists(Throwable t) {
      return ((t instanceof KeeperException) && ((KeeperException) t).code() == KeeperException.Code.NONODE);
    }

    private void waitExists(String path) {
      LOG.info("Path " + path + " not exists. Watch for creation.");

      // If the node doesn't exists, use the "exists" call to watch for node creation.
      Futures.addCallback(zkClient.exists(path, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (event.getType() == Event.EventType.NodeCreated || event.getType() == Event.EventType.NodeDeleted) {
            action.run();
          }
        }
      }), new FutureCallback<Stat>() {
        @Override
        public void onSuccess(Stat result) {
          // If path exists, get children again, otherwise wait for watch to get triggered
          if (result != null) {
            action.run();
          }
        }
        @Override
        public void onFailure(Throwable t) {
          action.run();
        }
      });
    }
  }

  private static final class BrokerInfo {
    private final String brokerId;
    private final int partitionSize;

    private BrokerInfo(String brokerId, int partitionSize) {
      this.brokerId = brokerId;
      this.partitionSize = partitionSize;
    }

    public String getBrokerId() {
      return brokerId;
    }

    public int getPartitionSize() {
      return partitionSize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      BrokerInfo that = (BrokerInfo) o;
      return brokerId.equals(that.getBrokerId());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(brokerId);
    }
  }
}
