package org.sagebionetworks.bridge.udd.config;

// TODO: This is copy-pasted from BridgePF. We should refactor these into a shared package.
public interface ConfigReader {
    String read(String name);
}
