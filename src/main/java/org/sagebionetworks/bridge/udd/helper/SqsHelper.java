package org.sagebionetworks.bridge.udd.helper;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.config.EnvironmentConfig;

/** Helper class to wrap polling SQS and deleting messages. */
@Component
public class SqsHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SqsHelper.class);

    // package-scoped to be available in unit tests
    static final String CONFIG_KEY_SQS_QUEUE_URL = "sqs.queue.url";

    private AmazonSQSClient sqsClient;
    private String sqsQueueUrl;

    /** Environment config, used to get SQS queue URL from configs. */
    @Autowired
    public final void setEnvConfig(EnvironmentConfig envConfig) {
        this.sqsQueueUrl = envConfig.getProperty(CONFIG_KEY_SQS_QUEUE_URL);

        // Unconventional, but super helpful in making sure we're configured properly
        LOG.info("Configured SQS queue: " + sqsQueueUrl);
    }

    /** SQS client. */
    @Autowired
    public final void setSqsClient(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Blocking call that polls SQS with a 20 second timeout. Returns at most 1 message. Returns null if no messages
     * are available within that 20 second timeout.
     *
     * @return an SQS message, or null if no messages are available
     */
    public Message poll() {
        ReceiveMessageResult sqsResult = sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(sqsQueueUrl).withMaxNumberOfMessages(1).withWaitTimeSeconds(20));

        List<Message> sqsMessageList = sqsResult.getMessages();
        int numMessages = sqsMessageList.size();
        if (numMessages == 0) {
            // Poll returned no messages. This is normal. Return null to signal no message.
            return null;
        } else if (numMessages > 1) {
            LOG.warn("Asked SQS for at most 1 message, but got " + numMessages +
                    ", ignoring all but the first");
        }

        Message sqsMessage = sqsMessageList.get(0);
        return sqsMessage;
    }

    /**
     * Deletes the message from SQS. Should only be called after processing is complete, to guarantee at-least-once
     * semantics.
     *
     * @param receiptHandle
     *         SQS message receipt handle
     */
    public void deleteMessage(String receiptHandle) {
        sqsClient.deleteMessage(sqsQueueUrl, receiptHandle);
    }
}
