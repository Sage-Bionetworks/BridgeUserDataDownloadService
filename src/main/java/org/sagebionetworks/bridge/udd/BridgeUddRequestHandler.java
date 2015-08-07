package org.sagebionetworks.bridge.udd;

public interface BridgeUddRequestHandler {
    void handle(BridgeUddRequestContext context) throws BridgeUddException;
}
