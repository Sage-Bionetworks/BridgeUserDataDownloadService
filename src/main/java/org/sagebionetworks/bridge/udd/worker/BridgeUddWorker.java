package org.sagebionetworks.bridge.udd.worker;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SqsHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;
import org.sagebionetworks.bridge.udd.util.BridgeUddUtil;

/**
 * Worker that loops and polls SQS for requests and processes those requests. This is prototype scoped so we can create
 * multiples of these for multi-threading.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class BridgeUddWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddWorker.class);

    // package-scoped for unit tests
    static final String CONFIG_KEY_WORKER_SLEEP_TIME_MILLIS = "worker.sleep.time.millis";

    private DynamoHelper dynamoHelper;
    private Config environmentConfig;
    private SesHelper sesHelper;
    private SqsHelper sqsHelper;
    private StormpathHelper stormpathHelper;
    private SynapsePackager synapsePackager;

    /** Dynamo DB helper, used to get study info and uploads. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /**
     * Environment config, used to get the sleep delay. Its primary purpose is to allow setting no sleep for unit
     * tests.
     */
    @Autowired
    public final void setEnvironmentConfig(Config environmentConfig) {
        this.environmentConfig = environmentConfig;
    }

    /** SES helper, used to email the pre-signed URL to the requesting user. */
    @Autowired
    public final void setSesHelper(SesHelper sesHelper) {
        this.sesHelper = sesHelper;
    }

    /** SQS helper. We poll this to get requests. */
    @Autowired
    public final void setSqsHelper(SqsHelper sqsHelper) {
        this.sqsHelper = sqsHelper;
    }

    /** Stormpath helper. Used to get the email address and health ID (and indirectly, health code) for a user. */
    @Autowired
    public final void setStormpathHelper(StormpathHelper stormpathHelper) {
        this.stormpathHelper = stormpathHelper;
    }

    /** Synapse packager. Used to query Synapse and package the results in an S3 pre-signed URL. */
    @Autowired
    public void setSynapsePackager(SynapsePackager synapsePackager) {
        this.synapsePackager = synapsePackager;
    }

    /** Main worker loop. */
    @Override
    public void run() {
        int sleepTimeMillis = environmentConfig.getInt(CONFIG_KEY_WORKER_SLEEP_TIME_MILLIS);

        while (shouldKeepRunning()) {
            // Without this sleep statement, really weird things happen when we Ctrl+C the process. (Not relevant for
            // production, but happens all the time for local testing.) Empirically, it takes up to 125ms for the JVM
            // to shut down cleanly.) Plus, it prevents us from polling the SQS queue too fast when there are a lot of
            // messages.
            if (sleepTimeMillis > 0) {
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
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

                String username = request.getUsername();
                int userHash = username.hashCode();
                String studyId = request.getStudyId();
                String startDateStr = request.getStartDate().toString();
                String endDateStr = request.getEndDate().toString();
                LOG.info("Received request for hash[username]=" + userHash + ", study=" + studyId + ", startDate=" +
                        startDateStr + ",endDate=" + endDateStr);

                Stopwatch requestStopwatch = Stopwatch.createStarted();
                try {
                    // We need the study, because accounts and data are partitioned on study.
                    StudyInfo studyInfo = dynamoHelper.getStudy(studyId);

                    AccountInfo accountInfo = stormpathHelper.getAccount(studyInfo, username);
                    String healthCode = dynamoHelper.getHealthCodeFromHealthId(accountInfo.getHealthId());
                    Map<String, UploadSchema> synapseToSchemaMap = dynamoHelper.getSynapseTableIdsForStudy(studyId);
                    PresignedUrlInfo presignedUrlInfo = synapsePackager.packageSynapseData(synapseToSchemaMap,
                            healthCode, request);

                    if (presignedUrlInfo == null) {
                        LOG.info("No data for request for hash[username]=" + userHash + ", study=" + studyId +
                                ", startDate=" + startDateStr + ",endDate=" + endDateStr);
                        sesHelper.sendNoDataMessageToAccount(studyInfo, accountInfo);
                    } else {
                        sesHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);
                    }

                    // We're done processing the SQS message. Delete it so it doesn't get duped.
                    sqsHelper.deleteMessage(sqsMessage.getReceiptHandle());
                } finally {
                    LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                            " seconds for hash[username]=" + userHash + ", study=" + studyId + ", startDate=" +
                            startDateStr + ",endDate=" + endDateStr);
                }
            } catch (Exception ex) {
                LOG.error("BridgeUddWorker exception: " + ex.getMessage(), ex);
            } catch (Error err) {
                LOG.error("BridgeUddWorker critical error: " + err.getMessage(), err);
            }
        }
    }

    // This is called by BridgeUddWorker for every loop iteration to determine if worker should keep running. This
    // is a member method to enable mocking and is package-scoped to make it available to unit tests.
    boolean shouldKeepRunning() {
        return true;
    }
}
