package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaClient;
import com.continuuity.kafka.client.PreparePublish;
import com.continuuity.zk.RetryStrategies;
import com.continuuity.zk.ZKClientService;
import com.continuuity.zk.ZKClientServices;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class SimpleKafkaClient extends AbstractIdleService implements KafkaClient {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaClient.class);
  private static final int BROKER_POLL_INTERVAL = 100;

  private final ZKClientService zkClientService;
  private final KafkaBrokerCache brokerCache;
//  private ClientBootstrap bootstrap;
//  private ChannelGroup channelGroup;
  private ConnectionPool connectionPool;

  public SimpleKafkaClient(String zkConnectStr) {
    zkClientService = ZKClientServices.reWatchOnExpire(
                        ZKClientServices.retryOnFailure(
                          ZKClientService.Builder.of(zkConnectStr).build(),
                                                     RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
    brokerCache = new KafkaBrokerCache(zkClientService);
  }

  @Override
  protected void startUp() throws Exception {
    zkClientService.startAndWait();
    brokerCache.startAndWait();
//    channelGroup = new DefaultChannelGroup();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
                                      .setDaemon(true)
                                      .setNameFormat("kafka-client-netty-%d")
                                      .build();

    ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                                                      Executors.newSingleThreadExecutor(threadFactory),
                                                      Executors.newFixedThreadPool(4, threadFactory)));
    bootstrap.setPipelineFactory(new KafkaChannelPipelineFactory());
    connectionPool = new ConnectionPool(bootstrap);
  }

  @Override
  protected void shutDown() throws Exception {
//    channelGroup.close();
    connectionPool.close();
//    bootstrap.releaseExternalResources();
    brokerCache.stopAndWait();
    zkClientService.stopAndWait();
  }

  @Override
  public PreparePublish preparePublish(final String topic, final Compression compression) {
    final Map<Integer, MessageSetEncoder> encoders = Maps.newHashMap();

    return new PreparePublish() {
      @Override
      public PreparePublish add(byte[] payload, Object partitionKey) {
        return add(ByteBuffer.wrap(payload), partitionKey);
      }

      @Override
      public PreparePublish add(ByteBuffer payload, Object partitionKey) {
        // TODO: Partition
        int partition = 0;

        MessageSetEncoder encoder = encoders.get(partition);
        if (encoder == null) {
          encoder = getEncoder(compression);
          encoders.put(partition, encoder);
        }
        encoder.add(ChannelBuffers.wrappedBuffer(payload));

        return this;
      }

      @Override
      public ListenableFuture<?> publish() {
        List<ListenableFuture<?>> futures = Lists.newArrayListWithCapacity(encoders.size());
        for (Map.Entry<Integer, MessageSetEncoder> entry : encoders.entrySet()) {
          futures.add(doPublish(topic, entry.getKey(), entry.getValue().finish()));
        }
        encoders.clear();
        return Futures.allAsList(futures);
      }

      private ListenableFuture<?> doPublish(String topic, int partition, ChannelBuffer messageSet) {
        final KafkaRequest request = KafkaRequest.createProduce(topic, partition, messageSet);
        final SettableFuture<?> result = SettableFuture.create();
        final ConnectionPool.ConnectResult connection =
              connectionPool.connect(getTopicBroker(topic, partition).getAddress());

        connection.getChannelFuture().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            try {
              future.getChannel().write(request).addListener(getPublishChannelFutureListener(result, null, connection));
            } catch (Exception e) {
              result.setException(e);
            }
          }
        });

        return result;
      }
    };
  }

  @Override
  public Iterator<FetchedMessage> consume(final String topic, final int partition, long offset, int maxSize) {
    Preconditions.checkArgument(maxSize >= 10, "Message size cannot be smaller than 10.");

    // Connect to broker. Consumer connection are long connection. No need to worry about reuse.
    final AtomicReference<ChannelFuture> channelFutureRef = new AtomicReference<ChannelFuture>(
          connectionPool.connect(getTopicBroker(topic, partition).getAddress()).getChannelFuture());

    return new MessageFetcher(topic, partition, offset, maxSize, new KafkaRequestSender() {

      @Override
      public void send(final KafkaRequest request) {
        try {
          // Try to send the request
          Channel channel = channelFutureRef.get().getChannel();
          if (!channel.write(request).await().isSuccess()) {
            // If failed, retry
            channel.close();
            ChannelFuture channelFuture = connectionPool.connect(
                                              getTopicBroker(topic, partition).getAddress()).getChannelFuture();
            channelFutureRef.set(channelFuture);
            channelFuture.addListener(new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                send(request);
              }
            });
          }
        } catch (InterruptedException e) {
          // Ignore it
          LOG.info("Interrupted when sending consume request", e);
        }
      }
    });
  }

//  private ChannelFuture connect(InetSocketAddress address) {
//    connectionPool.connect(address)
//    result.addListener(new ChannelFutureListener() {
//      @Override
//      public void operationComplete(ChannelFuture channelFuture) throws Exception {
//        if (channelFuture.isSuccess()) {
//          channelGroup.add(channelFuture.getChannel());
//        }
//      }
//    });
//    return result;
//  }

  private TopicBroker getTopicBroker(String topic, int partition) {
    TopicBroker topicBroker = brokerCache.getBrokerAddress(topic, partition);
    while (topicBroker == null) {
      try {
        TimeUnit.MILLISECONDS.sleep(BROKER_POLL_INTERVAL);
      } catch (InterruptedException e) {
        return null;
      }
      topicBroker = brokerCache.getBrokerAddress(topic, partition);
    }
    return topicBroker;
  }

  private MessageSetEncoder getEncoder(Compression compression) {
    switch (compression) {
      case GZIP:
        return new GZipMessageSetEncoder();
      case SNAPPY:
//        break;
      default:
        return new IdentityMessageSetEncoder();
    }
  }

  private <V> ChannelFutureListener getPublishChannelFutureListener(final SettableFuture<V> result, final V resultObj,
                                                                    final ConnectionPool.ConnectionReleaser releaser) {
    return new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        try {
          if (future.isSuccess()) {
            result.set(resultObj);
          } else if (future.isCancelled()) {
            result.cancel(true);
          } else {
            result.setException(future.getCause());
          }
        } finally {
          releaser.release();
        }
      }
    };
  }

  private static final class KafkaChannelPipelineFactory implements ChannelPipelineFactory {

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();

      pipeline.addLast("encoder", new KafkaRequestEncoder());
      pipeline.addLast("decoder", new KafkaResponseHandler());
      pipeline.addLast("dispatcher", new KafkaResponseDispatcher());
      return pipeline;
    }
  }
}
