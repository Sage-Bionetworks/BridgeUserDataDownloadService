package org.sagebionetworks.bridge.udd.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.worker.BridgeUddWorker;

@Component
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    private ApplicationContext springCtx;

    @Autowired
    public void setSpringContext(ApplicationContext springCtx) {
        this.springCtx = springCtx;
    }

    @Override
    public void run(String... args) {
        LOG.info("Launching workers...");

        // We use getBean() instead of autowiring because this is a prototype bean, so we actually actually want to
        // get multiple copies of this bean for our executor.
        // TODO implement executor and multithreading
        BridgeUddWorker worker = springCtx.getBean(BridgeUddWorker.class);
        worker.run();
    }
}
