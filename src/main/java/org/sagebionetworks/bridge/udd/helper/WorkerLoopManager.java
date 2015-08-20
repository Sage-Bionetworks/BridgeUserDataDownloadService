package org.sagebionetworks.bridge.udd.helper;

import org.springframework.stereotype.Component;

/**
 * Loop manager for the BridgeUddWorker. shouldKeepRunning() will always return true. Mock this in unit tests to
 * enforce a limited number of loop iterations.
 */
@Component
public class WorkerLoopManager {
    /**
     * This is called by BridgeUddWorker for every loop iteration to determine if worker should keep running.
     * Subclasses and test mocks should override this method to specify a fixed number of loops.
     *
     * @return true if the worker should keep running
     */
    public boolean shouldKeepRunning() {
        return true;
    }
}
