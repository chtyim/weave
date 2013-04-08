package com.continuuity.weave.internal.container;

import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.ServiceMain;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public final class WeaveContainerMain extends ServiceMain {

  /**
   *
   * @param args 0 - zkStr, 1 - spec.json, 2 - runnable name
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    // TODO: Use Jar class loader
    WeaveSpecification weaveSpec = loadWeaveSpec(args[1]);
    WeaveRunnableSpecification runnableSpec = weaveSpec.getRunnables().get(args[2]).getRunnableSpecification();
    new WeaveContainerMain().doMain(new WeaveContainer(args[0], runnableSpec, ClassLoader.getSystemClassLoader()));
  }

  private static WeaveSpecification loadWeaveSpec(String spec) throws IOException {
    Reader reader = Files.newReader(new File(spec), Charsets.UTF_8);
    try {
      return WeaveSpecificationAdapter.create().fromJson(reader);
    } finally {
      reader.close();
    }
  }
}
