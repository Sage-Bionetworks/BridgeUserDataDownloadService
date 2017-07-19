package org.sagebionetworks.bridge.udd.worker;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.BridgeHelper;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;

/** SQS callback. Called by the PollSqsWorker. This handles a UDD request. */
@Component
public class BridgeUddProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddProcessor.class);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private SesHelper sesHelper;
    private StormpathHelper stormpathHelper;
    private SynapsePackager synapsePackager;

    /** Bridge helper, used to call Bridge server to get account info, such as email address and health code. */
    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    /** Dynamo DB helper, used to get study info and uploads. */
    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** SES helper, used to email the pre-signed URL to the requesting user. */
    @Autowired
    public final void setSesHelper(SesHelper sesHelper) {
        this.sesHelper = sesHelper;
    }

    /** Stormpath helper. Used to get the email address and health ID (and indirectly, health code) for a user. */
    @Autowired
    public final void setStormpathHelper(StormpathHelper stormpathHelper) {
        this.stormpathHelper = stormpathHelper;
    }

    /** Synapse packager. Used to query Synapse and package the results in an S3 pre-signed URL. */
    @Autowired
    public final void setSynapsePackager(SynapsePackager synapsePackager) {
        this.synapsePackager = synapsePackager;
    }

    public void process(JsonNode body) throws IOException, PollSqsWorkerBadRequestException {
        BridgeUddRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(body, BridgeUddRequest.class);
        } catch (IOException ex) {
            throw new PollSqsWorkerBadRequestException("Error parsing request: " + ex.getMessage(), ex);
        }

        String userId = request.getUserId();
        String username = request.getUsername();
        Integer userHash = username != null ? username.hashCode() : null;
        String studyId = request.getStudyId();
        String startDateStr = request.getStartDate().toString();
        String endDateStr = request.getEndDate().toString();
        LOG.info("Received request for userId=" + userId + ", hash[username]=" + userHash + ", study="
                + studyId + ", startDate=" + startDateStr + ",endDate=" + endDateStr);

        Stopwatch requestStopwatch = Stopwatch.createStarted();
        try {
            // We need the study, because accounts and data are partitioned on study.
            StudyInfo studyInfo = dynamoHelper.getStudy(studyId);

            AccountInfo accountInfo;
            String healthCode;
            if (StringUtils.isNotBlank(userId)) {
                accountInfo = bridgeHelper.getAccountInfo(studyId, userId);
                healthCode = accountInfo.getHealthCode();
            } else {
                accountInfo = stormpathHelper.getAccount(studyInfo, username);
                healthCode = dynamoHelper.getHealthCodeFromHealthId(accountInfo.getHealthId());
            }
            if (healthCode == null) {
                throw new PollSqsWorkerBadRequestException("Health code not found for account " +
                        accountInfo.getLogId());
            }

            Map<String, UploadSchema> synapseToSchemaMap = dynamoHelper.getSynapseTableIdsForStudy(studyId);
            Set<String> surveyTableIdSet = dynamoHelper.getSynapseSurveyTablesForStudy(studyId);
            PresignedUrlInfo presignedUrlInfo = synapsePackager.packageSynapseData(synapseToSchemaMap,
                    healthCode, request, surveyTableIdSet);

            if (presignedUrlInfo == null) {
                LOG.info("No data for request for account " + accountInfo.getLogId() + ", study=" + studyId +
                        ", startDate=" + startDateStr + ",endDate=" + endDateStr);
                sesHelper.sendNoDataMessageToAccount(studyInfo, accountInfo);
            } else {
                sesHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);
            }
        } catch (BridgeSDKException ex) {
            int status = ex.getStatusCode();
            if (status >= 400 && status < 500) {
                throw new PollSqsWorkerBadRequestException(ex);
            } else {
                throw new RuntimeException(ex);
            }
        } finally {
            LOG.info("Request took " + requestStopwatch.elapsed(TimeUnit.SECONDS) +
                    " seconds for userId=" + userId + ", hash[username]=" + userHash + ", study=" + studyId +
                    ", startDate=" + startDateStr + ",endDate=" + endDateStr);
        }
    }
}
