package com.continuuity.weave;

import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 *
 */
public final class DistributedShell extends AbstractWeaveRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedShell.class);

  public DistributedShell(String...commands) {
    super(ImmutableMap.of("cmds", Joiner.on(';').join(commands)));
  }

  @Override
  public void run() {
    for (String cmd : Splitter.on(';').split(getArgument("cmds"))) {
      try {
        Process process = new ProcessBuilder(ImmutableList.copyOf(Splitter.on(' ').split(cmd)))
                              .redirectErrorStream(true).start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), Charsets.US_ASCII));
        try {
          String line = reader.readLine();
          while (line != null) {
            LOG.info(line);
            line = reader.readLine();
          }
        } finally {
          reader.close();
        }
      } catch (IOException e) {
        LOG.error("Fail to execute command " + cmd, e);
      }
    }
  }

  @Override
  public void stop() {
    // No-op
  }
}
