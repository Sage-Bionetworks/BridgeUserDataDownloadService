package org.sagebionetworks.bridge.udd.config;

// TODO: This is copy-pasted from BridgePF. We should refactor these into a shared package.
public interface ConfigReader {
    public String read(String name);
}
