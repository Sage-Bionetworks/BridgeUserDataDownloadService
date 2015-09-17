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
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.helper.SqsHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;

public class BridgeUddWorkerTest {
    // TODO un-ignore this
    @Test(enabled = false)
    public void test() throws Exception {
        // Overall test strategy: 2 loop iterations.
        // #1 - No SQS response, to test that we handle this.
        // #2 - SQS response and test the data flow

        // mock objects - These are used only as passthroughs between the sub-components. So just create mocks instead
        // of instantiating all the fields.
        StudyInfo mockStudyInfo = mock(StudyInfo.class);
        AccountInfo mockAccountInfo = mock(AccountInfo.class);
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

        // mock SES helper
        SesHelper mockSesHelper = mock(SesHelper.class);

        // set up test worker
        BridgeUddWorker testWorker = spy(new BridgeUddWorker());
        testWorker.setDynamoHelper(mockDynamoHelper);
        testWorker.setEnvironmentConfig(mockEnvConfig);
        testWorker.setSesHelper(mockSesHelper);
        testWorker.setSqsHelper(mockSqsHelper);
        testWorker.setStormpathHelper(mockStormpathHelper);

        // spy shouldKeepRunning() - 2 iterations
        doReturn(true).doReturn(true).doReturn(false).when(testWorker).shouldKeepRunning();

        // execute
        testWorker.run();

        // verify SesHelper call
        verify(mockSesHelper).sendPresignedUrlToAccount(same(mockStudyInfo), same(mockPresignedUrlInfo),
                same(mockAccountInfo));

        // verify we deleted the message
        verify(mockSqsHelper).deleteMessage("test-receipt-handle");
    }
}
