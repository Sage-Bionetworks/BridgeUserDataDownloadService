package org.sagebionetworks.bridge.udd.worker;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SqsHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.s3.S3Packager;

public class BridgeUddWorkerTest {
    // TODO un-ignore this
    @Test
    @Ignore
    public void test() throws Exception {
        // Overall test strategy: 2 loop iterations.
        // #1 - No SQS response, to test that we handle this.
        // #2 - SQS response and test the data flow

        // mock objects - These are used only as passthroughs between the sub-components. So just create mocks instead
        // of instantiating all the fields.
        StudyInfo mockStudyInfo = mock(StudyInfo.class);
        AccountInfo mockAccountInfo = mock(AccountInfo.class);
        List<UploadInfo> mockUploadInfoList = ImmutableList.of();
        PresignedUrlInfo mockPresignedUrlInfo = mock(PresignedUrlInfo.class);

        // mock env config - set sleep time to zero so we don't needlessly sleep in unit tests
        Config mockEnvConfig = mock(Config.class);
        when(mockEnvConfig.getInt(BridgeUddWorker.CONFIG_KEY_WORKER_SLEEP_TIME_MILLIS)).thenReturn(0);

        // mock SQS helper - first message null; second message is the request
        String secondSqsMessageBody = "{\n" +
                "   \"studyId\":\"test-study\",\n" +
                "   \"username\":\"sqs-user\",\n" +
                "   \"startDate\":\"2015-08-19\",\n" +
                "   \"endDate\":\"2015-08-21\"\n" +
                "}";
        Message secondSqsMessage = new Message().withBody(secondSqsMessageBody)
                .withReceiptHandle("test-receipt-handle");
        SqsHelper mockSqsHelper = mock(SqsHelper.class);
        when(mockSqsHelper.poll()).thenReturn(null, secondSqsMessage);

        // mock dynamo helper - first call is to get the study
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudy("test-study")).thenReturn(mockStudyInfo);

        // mock stormpath helper
        StormpathHelper mockStormpathHelper = mock(StormpathHelper.class);
        when(mockStormpathHelper.getAccount(same(mockStudyInfo), eq("sqs-user"))).thenReturn(mockAccountInfo);

        // mock dynamo helper again - second call is to get the uploads
        ArgumentCaptor<BridgeUddRequest> ddbRequestArgCaptor = ArgumentCaptor.forClass(BridgeUddRequest.class);
        when(mockDynamoHelper.getUploadsForRequest(same(mockAccountInfo), ddbRequestArgCaptor.capture()))
                .thenReturn(mockUploadInfoList);

        // mock S3 Packager
        ArgumentCaptor<BridgeUddRequest> s3PackagerRequestArgCaptor = ArgumentCaptor.forClass(BridgeUddRequest.class);
        S3Packager mockS3Packager = mock(S3Packager.class);
        when(mockS3Packager.packageFilesForUploadList(s3PackagerRequestArgCaptor.capture(), same(mockUploadInfoList)))
                .thenReturn(mockPresignedUrlInfo);

        // mock SES helper
        SesHelper mockSesHelper = mock(SesHelper.class);

        // set up test worker
        BridgeUddWorker testWorker = spy(new BridgeUddWorker());
        testWorker.setDynamoHelper(mockDynamoHelper);
        testWorker.setEnvironmentConfig(mockEnvConfig);
        testWorker.setS3Packager(mockS3Packager);
        testWorker.setSesHelper(mockSesHelper);
        testWorker.setSqsHelper(mockSqsHelper);
        testWorker.setStormpathHelper(mockStormpathHelper);

        // spy shouldKeepRunning() - 2 iterations
        doReturn(true).doReturn(true).doReturn(false).when(testWorker).shouldKeepRunning();

        // execute
        testWorker.run();

        // validate BridgeUddRequest was parsed and passed around correctly
        BridgeUddRequest ddbRequest = ddbRequestArgCaptor.getValue();
        assertEquals(ddbRequest.getStudyId(), "test-study");
        assertEquals(ddbRequest.getUsername(), "sqs-user");
        assertEquals(ddbRequest.getStartDate().toString(), "2015-08-19");
        assertEquals(ddbRequest.getEndDate().toString(), "2015-08-21");

        // request sent to DDB is the same object as the one sent to the S3 Packager
        assertSame(s3PackagerRequestArgCaptor.getValue(), ddbRequest);

        // verify SesHelper call
        verify(mockSesHelper).sendPresignedUrlToAccount(same(mockStudyInfo), same(mockPresignedUrlInfo),
                same(mockAccountInfo));

        // verify we deleted the message
        verify(mockSqsHelper).deleteMessage("test-receipt-handle");
    }
}
