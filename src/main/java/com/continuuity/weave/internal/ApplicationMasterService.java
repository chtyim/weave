package com.continuuity.weave.internal;

import com.continuuity.weave.api.WeaveApplicationSpecification;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 */
public class ApplicationMasterService extends AbstractExecutionThreadService {

  private final WeaveApplicationSpecification appSpec;
  private final AMRMClient amrmClient;

  private volatile Thread runThread;

  public ApplicationMasterService(WeaveApplicationSpecification appSpec) {
    this.appSpec = appSpec;

    // Get the container ID and convert it to ApplicationAttemptId
    String containerIdString = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(containerIdString != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(containerIdString).getApplicationAttemptId());
  }

  @Override
  protected void startUp() throws Exception {
    amrmClient.init(new Configuration());
    amrmClient.start();
  }

  @Override
  protected void shutDown() throws Exception {
    amrmClient.stop();
  }

  @Override
  protected void triggerShutdown() {
    Thread runThread = this.runThread;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void run() throws Exception {
    runThread = Thread.currentThread();

    while (isRunning()) {

    }
  }
}
