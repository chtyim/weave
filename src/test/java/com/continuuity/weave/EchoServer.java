package com.continuuity.weave;

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 */
public final class EchoServer extends AbstractWeaveRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);

  private volatile boolean running;
  private volatile Thread runThread;

  public EchoServer(int port) {
    super(ImmutableMap.of("port", Integer.toString(port)));
  }

  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);
    running = true;
  }

  @Override
  public void run() {
    try {
      runThread = Thread.currentThread();
      ServerSocket serverSocket = new ServerSocket(Integer.parseInt(getArgument("port")));

      while (running) {
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));
        try {
          BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
          try {
            String line = reader.readLine();
            LOG.info("Received: " + line);
            if (line != null) {
              writer.write(line);
            }
          } finally {
            writer.close();
          }
        } finally {
          reader.close();
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    running = false;
    Thread t = runThread;
    if (t != null) {
      t.interrupt();
    }
  }
}
