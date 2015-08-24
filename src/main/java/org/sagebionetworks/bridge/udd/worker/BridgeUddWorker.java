package org.sagebionetworks.bridge.udd.worker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.config.EnvironmentConfig;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SqsHelper;
import org.sagebionetworks.bridge.udd.helper.WorkerLoopManager;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.s3.S3Packager;
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
    private EnvironmentConfig environmentConfig;
    private WorkerLoopManager loopManager;
    private S3Packager s3Packager;
    private SesHelper sesHelper;
    private SqsHelper sqsHelper;
    private StormpathHelper stormpathHelper;

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
    public final void setEnvironmentConfig(EnvironmentConfig environmentConfig) {
        this.environmentConfig = environmentConfig;
    }

    /**
     * Loop manager, to determine how long to loop for. Its primary purpose is to allow a fixed number of repetitions
     * for a unit test.
     */
    @Autowired
    public final void setLoopManager(WorkerLoopManager loopManager) {
        this.loopManager = loopManager;
    }

    /**
     * S3 Packager, which packages uploads into a master zip file, uploads the zip file to S3, and generates and
     * returns an S3 pre-signed URL.
     */
    @Autowired
    public final void setS3Packager(S3Packager s3Packager) {
        this.s3Packager = s3Packager;
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

    /** Main worker loop. */
    @Override
    public void run() {
        int sleepTimeMillis = environmentConfig.getPropertyAsInt(CONFIG_KEY_WORKER_SLEEP_TIME_MILLIS);

        while (loopManager.shouldKeepRunning()) {
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

                String startDateStr = request.getStartDate().toString();
                String endDateStr = request.getEndDate().toString();
                LOG.info("Received request for hash[username]=" + request.getUsername().hashCode() + ", study=" +
                        request.getStudyId() + ", startDate=" + startDateStr + ",endDate=" + endDateStr);

                Stopwatch requestStopwatch = Stopwatch.createStarted();
                try {
                    // This sequence of helpers does the following:
                    //  * get the study from DDB (because accounts are partitioned on Study)
                    //  * get the account from Stormpath
                    //  * get the upload metadata from DDB
                    //  * download the uploads, zip then, and write them back to S3
                    //  * email the S3 link to the user
                    StudyInfo studyInfo = dynamoHelper.getStudy(request.getStudyId());
                    AccountInfo accountInfo = stormpathHelper.getAccount(studyInfo, request.getUsername());
                    List<UploadInfo> uploadInfoList = dynamoHelper.getUploadsForRequest(accountInfo, request);
                    PresignedUrlInfo presignedUrlInfo = s3Packager.packageFilesForUploadList(request, uploadInfoList);
                    sesHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);

                    // We're done processing the SQS message. Delete it so it doesn't get duped.
                    sqsHelper.deleteMessage(sqsMessage.getReceiptHandle());
                } finally {
                    LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                            " seconds for hash[username]=" + request.getUsername().hashCode() + ", study=" +
                            request.getStudyId() + ", startDate=" + startDateStr + ",endDate=" + endDateStr);
                }
            } catch (Exception ex) {
                LOG.error("BridgeUddWorker exception: " + ex.getMessage(), ex);
            } catch (Error err) {
                LOG.error("BridgeUddWorker critical error: " + err.getMessage(), err);
            }
        }
    }
}
