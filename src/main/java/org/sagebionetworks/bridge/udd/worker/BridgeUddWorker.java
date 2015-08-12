package org.sagebionetworks.bridge.udd.worker;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.helper.SqsHelper;
import org.sagebionetworks.bridge.udd.helper.WorkerLoopManager;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.util.BridgeUddUtil;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BridgeUddWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddWorker.class);

    private DynamoHelper dynamoHelper;
    private WorkerLoopManager loopManager;
    private SqsHelper sqsHelper;
    private StormpathHelper stormpathHelper;

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setLoopManager(WorkerLoopManager loopManager) {
        this.loopManager = loopManager;
    }

    @Autowired
    public final void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    @Autowired
    public final void setStormpathHelper(StormpathHelper stormpathHelper) {
        this.stormpathHelper = stormpathHelper;
    }

    @Override
    public void run() {
        while (loopManager.shouldKeepRunning()) {

            // Without this sleep statement, really weird things happen when we Ctrl+C the process. (Empirically, it takes
            // up to 125ms for the JVM to shut down cleanly.) Plus, it prevents us from polling the SQS queue too fast
            // when there are a lot of messages.
            try {
                // TODO: move this 125ms to config
                Thread.sleep(125);
            } catch (InterruptedException ex) {
                LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
            }

            try {
                // get request from SQS
                Message sqsMessage = sqsHelper.poll();
                if (sqsMessage == null) {
                    // No messages yet. Loop around again.
                    continue;
                }
                String sqsMessageText = sqsMessage.getBody();
                BridgeUddRequest request = BridgeUddUtil.JSON_OBJECT_MAPPER.readValue(sqsMessageText,
                        BridgeUddRequest.class);

                StudyInfo studyInfo = dynamoHelper.getStudy(request.getStudyId());
                AccountInfo accountInfo = stormpathHelper.getAccount(studyInfo, request.getUsername());
                List<UploadInfo> uploadInfoList = dynamoHelper.getUploadsForRequest(accountInfo, request);

                // debug log
                List<String> uploadIdList = new ArrayList<>();
                for (UploadInfo oneUploadInfo : uploadInfoList) {
                    uploadIdList.add(oneUploadInfo.getId());
                }
                LOG.info("Found upload IDs: " + Joiner.on(", ").join(uploadIdList));

                // TODO: download from S3, package, upload back to S3, and send email

                // We're done processing the SQS message. Delete it so it doesn't get duped.
                sqsHelper.deleteMessage(sqsMessage.getReceiptHandle());
            } catch (Exception ex) {
                LOG.error("BridgeUddWorker exception: " + ex.getMessage(), ex);
            } catch (Error err) {
                LOG.error("BridgeUddWorker critical error: " + err.getMessage(), err);
            }
        }
    }
}
