package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class WorkerApp implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerApp.class);

    @Override
    public void run(String... args) {
        LOG.info("Running worker app...");
    }
}
