package org.apache.rya.kafka.base;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedKafkaSingleton {

    public static EmbeddedKafkaInstance getInstance() {
        return InstanceHolder.SINGLETON.instance;
    }

    private EmbeddedKafkaSingleton() {
        // hiding implicit default constructor
    }

    private enum InstanceHolder {

        SINGLETON;

        private final Logger log;
        private final EmbeddedKafkaInstance instance;

        InstanceHolder() {
            this.log = LoggerFactory.getLogger(EmbeddedKafkaInstance.class);
            this.instance = new EmbeddedKafkaInstance();
            try {
                this.instance.startup();

                // JUnit does not have an overall lifecycle event for tearing down
                // this kind of resource, but shutdown hooks work alright in practice
                // since this should only be used during testing

                // The only other alternative for lifecycle management is to use a
                // suite lifecycle to enclose the tests that need this resource.
                // In practice this becomes unwieldy.

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            InstanceHolder.this.instance.shutdown();
                        } catch (final Throwable t) {
                            // logging frameworks will likely be shut down
                            t.printStackTrace(System.err);
                        }
                    }
                });

            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while starting mini accumulo", e);
            } catch (final IOException e) {
                log.error("Unexpected error while starting mini accumulo", e);
            } catch (final Throwable e) {
                // catching throwable because failure to construct an enum
                // instance will lead to another error being thrown downstream
                log.error("Unexpected throwable while starting mini accumulo", e);
            }
        }
    }
}