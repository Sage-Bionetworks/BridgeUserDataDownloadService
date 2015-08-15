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

@Component
public class SqsHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SqsHelper.class);

    private EnvironmentConfig envConfig;
    private AmazonSQSClient sqsClient;

    @Autowired
    public void setEnvConfig(EnvironmentConfig envConfig) {
        this.envConfig = envConfig;
        LOG.info("Configured SQS queue: " + envConfig.getProperty("sqs.queue.url"));
    }

    @Autowired
    public void setSqsClient(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    public Message poll() {
        String queueUrl = envConfig.getProperty("sqs.queue.url");
        ReceiveMessageResult sqsResult = sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(queueUrl).withMaxNumberOfMessages(1).withWaitTimeSeconds(20));

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

    public void deleteMessage(String receiptHandle) {
        String queueUrl = envConfig.getProperty("sqs.queue.url");
        sqsClient.deleteMessage(queueUrl, receiptHandle);
    }
}
