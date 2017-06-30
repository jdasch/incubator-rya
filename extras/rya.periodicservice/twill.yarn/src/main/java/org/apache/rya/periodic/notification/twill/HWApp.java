package org.apache.rya.periodic.notification.twill;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.ext.BundledJarRunnable;
import org.apache.twill.ext.BundledJarRunner;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
/*
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
#import org.apache.rya.periodic.notification.twill.HelloWorld.HWRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
*/
public class HWApp {

	private static final Logger logger = LoggerFactory.getLogger(HWApp.class);




  /**
   * BundledJarApplication that specifies a single instance of main.sample.Scratch
   * to be run from a bundled jar.
   */
/*
  private static class ExampleBundledJarApp implements TwillApplication {
    private final String jarName;
    private final URI jarURI;

    public ExampleBundledJarApp(String jarName, URI jarURI) {
      this.jarName = jarName;
      this.jarURI = jarURI;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ExampleBundedJarApp")
        .withRunnable()
        .add("BundledJarRunnable", new BundledJarRunnable())
        .withLocalFiles()
        .add(jarName, jarURI, false)
        .apply()
        .anyOrder()
        .build();
    }
  }

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Arguments format: <host:port of zookeeper server>"
                           + " <bundle jar path> <main class name> <extra args>");
      System.exit(1);
    }

    String zkStr = args[0];
    BundledJarRunner.Arguments arguments = new BundledJarRunner.Arguments(
            args[1], "/lib", args[2], Arrays.copyOfRange(args, 3, args.length));

    File jarFile = new File(arguments.getJarFileName());
    Preconditions.checkState(jarFile.exists());
    Preconditions.checkState(jarFile.canRead());

    final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), zkStr);
    twillRunner.start();


    final TwillController controller = twillRunner.prepare(
      new ExampleBundledJarApp(jarFile.getName(), jarFile.toURI()))
      .withArguments("BundledJarRunnable", arguments.toArray())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
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
    } catch (ExecutionException e) {
      LOG.error("Error", e);
    }
}

*/








/*
	public static void main(final String[] args) {
		final TwillRunnerService twillRunner = new YarnTwillRunnerService(new YarnConfiguration(), args[0]);
		logger.info("starting twill runner...");
		twillRunner.start();
//
//
//		twillRunner.prepare(null, ResourceSpecification.Builder.with()
//				.setVirtualCores(1)
//				.setMemory(512, SizeUnit.MEGA)
//				.setInstances(2)
//				.setUplink(uplink, unit)
//				)

		logger.info("starting controller...");
		final TwillController controller = twillRunner.prepare(new HWRunnable())
				.addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
				.start();

		try {
			logger.info("waiting term");
			//controller.terminate().get();
			controller.awaitTerminated();

			logger.info("term");
			//controller.awaitTerminated();
		} catch (final ExecutionException e) {
			logger.warn("Exception occurred", e);
		}
	}
*/

}



