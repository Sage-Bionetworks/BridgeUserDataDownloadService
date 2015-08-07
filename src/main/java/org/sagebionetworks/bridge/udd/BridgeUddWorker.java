package org.sagebionetworks.bridge.udd;

import java.util.List;

import javax.annotation.Resource;

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BridgeUddWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddWorker.class);

    private List<BridgeUddRequestHandler> handerList;
    private BridgeUddDynamoExporter dynamoExporter;
    private SqsHelper sqsHelper;

    @Resource(name = "handlerList")
    public void setHanderList(List<BridgeUddRequestHandler> handerList) {
        this.handerList = handerList;
    }

    @Autowired
    public void setDynamoExporter(BridgeUddDynamoExporter dynamoExporter) {
        this.dynamoExporter = dynamoExporter;
    }

    @Autowired
    public void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    @Override
    public void run() {
        workerLoop:
        while (true) {
            // new iteration, new context
            BridgeUddRequestContext context = new BridgeUddRequestContext();

            // loop through handlers
            requestLoop:
            for (BridgeUddRequestHandler oneHandler : handerList) {
                String handlerName = oneHandler.getClass().getName();

                try {
                    oneHandler.handle(context);
                } catch (Throwable t) {
                    // TODO


                    if (t instanceof Error) {
                        LOG.error("Critical error processing handlerName");
                    }
                }
            }

            try {
                // Without this sleep statement, really weird things happen when we Ctrl+C the process. (Empirically,
                // it takes up to 125ms for the JVM to shut down cleanly.) Plus, it prevents us from polling the SQS
                // queue too fast when there are a lot of messages.
                try {
                    Thread.sleep(125);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }

                // get request from SQS
                Message sqsMessage = sqsHelper.poll();
                if (sqsMessage == null) {
                    // No messages yet. Loop around again.
                    continue;
                }
                String sqsMessageText = sqsMessage.getBody();
                BridgeUddRequest request = BridgeUddUtil.JSON_OBJECT_MAPPER.readValue(sqsMessageText,
                        BridgeUddRequest.class);

                // export from DynamoDB to S3
                dynamoExporter.exportForRequest(request);

                // We're done processing the SQS message. Delete it so it doesn't get duped.
                sqsHelper.deleteMessage(sqsMessage.getReceiptHandle());
            } catch (Exception ex) {
                LOG.error("BridgeUddWorker error: " + ex.getMessage(), ex);
            }
        }
    }
}
