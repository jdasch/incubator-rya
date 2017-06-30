package org.apache.rya.periodic.notification.twill;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.rya.periodic.notification.twill.HWorld.HWRunnable;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

/**
 * Hello World example using twill-yarn to run a TwillApplication over YARN.
 */
public class HWAppShade {

  public static final Logger LOG = LoggerFactory.getLogger(HWAppShade.class);

  public static void main(final String[] args) {
    if (args.length < 1) {
      System.err.println("Arguments format: <host:port of zookeeper server>");
      System.exit(1);
    }

    final String zkStr = args[0];
    final YarnConfiguration yarnConfiguration = new YarnConfiguration();
    final TwillRunnerService twillRunner = new YarnTwillRunnerService(yarnConfiguration, zkStr);
    twillRunner.start();

    final String yarnClasspath =
      yarnConfiguration.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                            Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
    final List<String> applicationClassPaths = Lists.newArrayList();
    Iterables.addAll(applicationClassPaths, Splitter.on(",").split(yarnClasspath));
    final TwillController controller = twillRunner.prepare(new HWRunnable())
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
        .withApplicationClassPaths(applicationClassPaths)
        //.withBundlerClassAcceptor(new HadoopClassExcluder())
        .start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Futures.getUnchecked(controller.terminate());
        } finally {
          twillRunner.stop();
        }
      }
    });

    try {
      controller.awaitTerminated();
    } catch (final ExecutionException e) {
      e.printStackTrace();
    }
  }

  static class HadoopClassExcluder extends ClassAcceptor {
    @Override
    public boolean accept(final String className, final URL classUrl, final URL classPathUrl) {
      // exclude hadoop but not hbase package
      return !(className.startsWith("org.apache.hadoop") && !className.startsWith("org.apache.hadoop.hbase"));
    }
  }
}