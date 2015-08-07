package org.sagebionetworks.bridge.udd;

import java.util.List;

public class BridgeUddRequestContext {
    private boolean shouldEndRequest = false;
    private boolean shouldEndWorker = false;
    private String sqsReceiptHandle;
    private BridgeUddRequest request;
    private List<String> uploadIdList;

    public boolean shouldEndRequest() {
        return shouldEndRequest;
    }

    public void setShouldEndRequest(boolean shouldEndRequest) {
        this.shouldEndRequest = shouldEndRequest;
    }

    public boolean shouldEndWorker() {
        return shouldEndWorker;
    }

    public void setShouldEndWorker(boolean shouldEndWorker) {
        this.shouldEndWorker = shouldEndWorker;
    }

    public String getSqsReceiptHandle() {
        return sqsReceiptHandle;
    }

    public void setSqsReceiptHandle(String sqsReceiptHandle) {
        this.sqsReceiptHandle = sqsReceiptHandle;
    }

    public BridgeUddRequest getRequest() {
        return request;
    }

    public void setRequest(BridgeUddRequest request) {
        this.request = request;
    }

    public List<String> getUploadIdList() {
        return uploadIdList;
    }

    public void setUploadIdList(List<String> uploadIdList) {
        this.uploadIdList = uploadIdList;
    }
}
