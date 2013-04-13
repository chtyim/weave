package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.weave.internal.utils.Threads;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 *
 */
final class MessageFetcher extends AbstractIterator<FetchedMessage> implements ResponseHandler {

  private static final long BACKOFF_INTERVAL_MS = 100;

  private final KafkaRequestSender sender;
  private final String topic;
  private final int partition;
  private final int maxSize;
  private final AtomicLong offset;
  private final BlockingQueue<FetchResult> messages;
  private final ScheduledExecutorService scheduler;
  private volatile long backoffMillis;
  private final Runnable sendFetchRequest = new Runnable() {
    @Override
    public void run() {
      sendFetchRequest();
    }
  };

  MessageFetcher(String topic, int partition, long offset, int maxSize, KafkaRequestSender sender) {
    this.topic = topic;
    this.partition = partition;
    this.sender = sender;
    this.offset = new AtomicLong(offset);
    this.maxSize = maxSize;
    this.messages = new LinkedBlockingQueue<FetchResult>();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
                        Threads.createDaemonThreadFactory("kafka-" + topic + "-consumer"));
  }

  @Override
  public void received(KafkaResponse response) {
    if (response.getErrorCode() != KafkaResponse.ErrorCode.OK) {
      messages.add(FetchResult.failure(new IllegalStateException("Error in fetching: " + response.getErrorCode())));
      return;
    }

    try {
      if (decodeResponse(response.getBody(), -1)) {
        backoffMillis = 0;
      } else {
        backoffMillis = Math.max(backoffMillis + BACKOFF_INTERVAL_MS, 1000);
        scheduler.schedule(sendFetchRequest, backoffMillis, TimeUnit.MILLISECONDS);
      }
    } catch (Throwable t) {
      messages.add(FetchResult.failure(t));
    }
  }

  private boolean decodeResponse(ChannelBuffer buffer, long nextOffset) {
    boolean hasMessage = false;
    boolean computeOffset = nextOffset < 0;
    while (buffer.readableBytes() >= 4) {
      int size = buffer.readInt();
      if (buffer.readableBytes() < size) {
        if (!hasMessage) {
          throw new IllegalStateException("Size too small");
        }
        break;
      }
      nextOffset = computeOffset ? offset.addAndGet(size + 4) : nextOffset;
      decodeMessage(size, buffer, nextOffset);
      hasMessage = true;
    }
    return hasMessage;

  }

  private void decodeMessage(int size, ChannelBuffer buffer, long nextOffset) {
    int readerIdx = buffer.readerIndex();
    int magic = buffer.readByte();
    Compression compression = magic == 0 ? Compression.NONE : Compression.fromCode(buffer.readByte());
    int crc = buffer.readInt();

    ChannelBuffer payload = buffer.readSlice(size - (buffer.readerIndex() - readerIdx));

    // TODO: verify CRC?
    enqueueMessage(compression, payload, nextOffset);
  }

  private void enqueueMessage(Compression compression, ChannelBuffer payload, long nextOffset) {
    switch (compression) {
      case NONE:
        messages.add(FetchResult.success(new BasicFetchedMessage(nextOffset, payload.toByteBuffer())));
        break;
      case GZIP:
        decodeResponse(gzipUncompress(payload), nextOffset);
        break;
      case SNAPPY:
        decodeResponse(snappyUncompress(payload), nextOffset);
        break;
    }
  }

  private ChannelBuffer gzipUncompress(ChannelBuffer source) {
    ChannelBufferOutputStream output = new ChannelBufferOutputStream(
                                              ChannelBuffers.dynamicBuffer(source.readableBytes() * 2));
    try {
      try {
        GZIPInputStream gzipInput = new GZIPInputStream(new ChannelBufferInputStream(source));
        try {
          ByteStreams.copy(gzipInput, output);
          return output.buffer();
        } finally {
          gzipInput.close();
        }
      } finally {
        output.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private ChannelBuffer snappyUncompress(ChannelBuffer source) {
    ChannelBufferOutputStream output = new ChannelBufferOutputStream(
                                              ChannelBuffers.dynamicBuffer(source.readableBytes() * 2));
    try {
      try {
        SnappyInputStream snappyInput = new SnappyInputStream(new ChannelBufferInputStream(source));
        try {
          ByteStreams.copy(snappyInput, output);
          return output.buffer();
        } finally {
          snappyInput.close();
        }
      } finally {
        output.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void sendFetchRequest() {
    ChannelBuffer fetchBody = ChannelBuffers.buffer(12);
    fetchBody.writeLong(offset.get());
    fetchBody.writeInt(maxSize);
    sender.send(KafkaRequest.createFetch(topic, partition, fetchBody, MessageFetcher.this));
  }

  @Override
  protected FetchedMessage computeNext() {
    FetchResult result = messages.poll();
    if (result != null) {
      return getMessage(result);
    }

    try {
      sendFetchRequest();
      return getMessage(messages.take());
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      return endOfData();
    }
  }

  private FetchedMessage getMessage(FetchResult result) {
    try {
      if (result.isSuccess()) {
        return result.getMessage();
      } else {
        throw result.getErrorCause();
      }
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private static final class FetchResult {
    private final FetchedMessage message;
    private final Throwable errorCause;

    static FetchResult success(FetchedMessage message) {
      return new FetchResult(message, null);
    }

    static FetchResult failure(Throwable cause) {
      return new FetchResult(null, cause);
    }

    private FetchResult(FetchedMessage message, Throwable errorCause) {
      this.message = message;
      this.errorCause = errorCause;
    }

    public FetchedMessage getMessage() {
      return message;
    }

    public Throwable getErrorCause() {
      return errorCause;
    }

    public boolean isSuccess() {
      return message != null;
    }
  }
}
