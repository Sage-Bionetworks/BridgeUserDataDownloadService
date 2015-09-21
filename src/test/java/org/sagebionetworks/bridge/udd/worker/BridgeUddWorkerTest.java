package org.sagebionetworks.bridge.udd.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.util.Map;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

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

public class BridgeUddWorkerTest {
    @Test
    public void test() throws Exception {
        // Overall test strategy: 2 loop iterations.
        // #1 - No SQS response, to test that we handle this.
        // #2 - no data from Synapse Packager
        // #3 - send pre-signed URL

        // mock objects - These are used only as passthroughs between the sub-components. So just create mocks instead
        // of instantiating all the fields.
        StudyInfo mockStudyInfo = mock(StudyInfo.class);
        Map<String, UploadSchema> mockSynapseToSchema = ImmutableMap.of();
        PresignedUrlInfo mockPresignedUrlInfo = mock(PresignedUrlInfo.class);

        // non-mock test objects - We break inside these objects to get data.
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("test@example.com")
                .withHealthId("test-health-id").withUsername("sqs-user").build();

        // mock env config - set sleep time to zero so we don't needlessly sleep in unit tests
        Config mockEnvConfig = mock(Config.class);
        when(mockEnvConfig.getInt(BridgeUddWorker.CONFIG_KEY_WORKER_SLEEP_TIME_MILLIS)).thenReturn(0);

        // mock SQS helper - first message null; second and third messages have data
        String sqsMsgBody2 = "{\n" +
                "   \"studyId\":\"test-study\",\n" +
                "   \"username\":\"sqs-user\",\n" +
                "   \"startDate\":\"2015-03-09\",\n" +
                "   \"endDate\":\"2015-03-31\"\n" +
                "}";
        Message sqsMsg2 = new Message().withBody(sqsMsgBody2).withReceiptHandle("receipt-handle-2");

        String sqsMsgBody3 = "{\n" +
                "   \"studyId\":\"test-study\",\n" +
                "   \"username\":\"sqs-user\",\n" +
                "   \"startDate\":\"2015-08-01\",\n" +
                "   \"endDate\":\"2015-08-31\"\n" +
                "}";
        Message sqsMsg3 = new Message().withBody(sqsMsgBody3).withReceiptHandle("receipt-handle-3");

        SqsHelper mockSqsHelper = mock(SqsHelper.class);
        when(mockSqsHelper.poll()).thenReturn(null, sqsMsg2, sqsMsg3);

        // mock dynamo helper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudy("test-study")).thenReturn(mockStudyInfo);
        when(mockDynamoHelper.getHealthCodeFromHealthId("test-health-id")).thenReturn("test-health-code");
        when(mockDynamoHelper.getSynapseTableIdsForStudy("test-study")).thenReturn(mockSynapseToSchema);

        // mock stormpath helper
        StormpathHelper mockStormpathHelper = mock(StormpathHelper.class);
        when(mockStormpathHelper.getAccount(same(mockStudyInfo), eq("sqs-user"))).thenReturn(accountInfo);

        // mock Synapse packager
        SynapsePackager mockPackager = mock(SynapsePackager.class);
        when(mockPackager.packageSynapseData(same(mockSynapseToSchema), eq("test-health-code"),
                any(BridgeUddRequest.class))).thenAnswer(invocation -> {
            // Second request is in March, that has no data. Third request is in August, that has data.
            BridgeUddRequest request = invocation.getArgumentAt(2, BridgeUddRequest.class);
            int startMonth = request.getStartDate().getMonthOfYear();
            switch (startMonth) {
                case 3:
                    return null;
                case 8:
                    return mockPresignedUrlInfo;
                default:
                    fail("unexpected month: " + startMonth);
                    // Need to return something anyway, because Java doesn't know fail always throws.
                    return null;
            }
        });

        // mock SES helper
        SesHelper mockSesHelper = mock(SesHelper.class);

        // set up test worker
        BridgeUddWorker testWorker = spy(new BridgeUddWorker());
        testWorker.setDynamoHelper(mockDynamoHelper);
        testWorker.setEnvironmentConfig(mockEnvConfig);
        testWorker.setSesHelper(mockSesHelper);
        testWorker.setSqsHelper(mockSqsHelper);
        testWorker.setStormpathHelper(mockStormpathHelper);
        testWorker.setSynapsePackager(mockPackager);

        // spy shouldKeepRunning() - 3 iterations
        doReturn(true).doReturn(true).doReturn(true).doReturn(false).when(testWorker).shouldKeepRunning();

        // execute
        testWorker.run();

        // verify SesHelper calls
        verify(mockSesHelper).sendNoDataMessageToAccount(same(mockStudyInfo), same(accountInfo));
        verify(mockSesHelper).sendPresignedUrlToAccount(same(mockStudyInfo), same(mockPresignedUrlInfo),
                same(accountInfo));

        // verify we deleted the messages
        verify(mockSqsHelper).deleteMessage("receipt-handle-2");
        verify(mockSqsHelper).deleteMessage("receipt-handle-3");
    }
}
