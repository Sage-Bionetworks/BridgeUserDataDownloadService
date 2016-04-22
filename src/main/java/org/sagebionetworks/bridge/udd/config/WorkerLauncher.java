package org.sagebionetworks.bridge.udd.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.udd.worker.BridgeUddWorker;

/**
 * Launches worker threads. This hooks into the Spring Boot command-line runner, which is really just a big
 * Runnable-equivalent that Spring Boot knows about.
 */
@Component
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    private HeartbeatLogger heartbeatLogger;
    private ApplicationContext springCtx;

    @Autowired
    public final void setHeartbeatLogger(HeartbeatLogger heartbeatLogger) {
        this.heartbeatLogger = heartbeatLogger;
    }

    /** Spring context. */
    @Autowired
    public final void setSpringContext(ApplicationContext springCtx) {
        this.springCtx = springCtx;
    }

    /**
     * Main entry point into the app. Should only be called by Spring Boot.
     *
     * @param args
     *         command-line args
     */
    @Override
    public void run(String... args) {
        LOG.info("Launching heartbeat...");
        new Thread(heartbeatLogger).start();

        LOG.info("Launching workers...");
        // We use getBean() instead of autowiring because this is a prototype bean, so we actually actually want to
        // get multiple copies of this bean for our executor.
        // TODO implement executor and multithreading
        BridgeUddWorker worker = springCtx.getBean(BridgeUddWorker.class);
        new Thread(worker).start();
    }
}
