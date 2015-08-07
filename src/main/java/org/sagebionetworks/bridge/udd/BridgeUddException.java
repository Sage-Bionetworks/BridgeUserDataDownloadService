package org.sagebionetworks.bridge.udd;

@SuppressWarnings("serial")
public class BridgeUddException extends Exception {
    public BridgeUddException() {
    }

    public BridgeUddException(String message) {
        super(message);
    }

    public BridgeUddException(String message, Throwable cause) {
        super(message, cause);
    }

    public BridgeUddException(Throwable cause) {
        super(cause);
    }
}
