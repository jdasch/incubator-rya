package org.apache.rya.periodic.notification.twill;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

public class HWorld {

	public static class HWRunnable extends AbstractTwillRunnable {
		private static final Logger LOG = LoggerFactory.getLogger(HWRunnable.class);

		public HWRunnable() {
		}

		@Override
		public void run() {
			try {
				for (int i = 0; i < 60; i++) {
					LOG.warn("Helloo World! -- " + i + " " + Hashing.adler32().hashInt(i).toString());
					Thread.sleep(2000);
				}
			} catch (final InterruptedException e) {
				LOG.warn("Exception occurred while sleeping", e);
			}
		}
	}
	public static class HWorldApp implements TwillApplication {

		@Override
		public TwillSpecification configure() {
			return TwillSpecification.Builder.with()
					.setName("HWorldApp")
					.withRunnable()
						.add("HWRunnable", new HWRunnable()).noLocalFiles()
					.anyOrder()
					.build();
		}

	}

	public static void main(final String[] a) {
		final HWRunnable r = new HWRunnable();
		r.run();
	}
}


