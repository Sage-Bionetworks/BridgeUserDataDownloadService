package org.sagebionetworks.bridge.udd.helper;

import org.springframework.stereotype.Component;

/**
 * Loop manager for the BridgeUddWorker. shouldKeepRunning() will always return true. Mock this in unit tests to
 * enforce a limited number of loop iterations.
 */
@Component
public class WorkerLoopManager {
    public boolean shouldKeepRunning() {
        return true;
    }
}
