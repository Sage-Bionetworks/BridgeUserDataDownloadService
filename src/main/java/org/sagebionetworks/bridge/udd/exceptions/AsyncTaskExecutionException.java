package org.sagebionetworks.bridge.udd.exceptions;

/**
 * This represents an exception in an asynchronous Runnable. This is a RuntimeException because Runnable doesn't
 * declare any checked exceptions. This is generally converted into an ExecutionException by the ExecutorService and
 * Future.get().
 */
@SuppressWarnings("serial")
public class AsyncTaskExecutionException extends RuntimeException {
    public AsyncTaskExecutionException() {
    }

    public AsyncTaskExecutionException(String message) {
        super(message);
    }

    public AsyncTaskExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public AsyncTaskExecutionException(Throwable cause) {
        super(cause);
    }
}
