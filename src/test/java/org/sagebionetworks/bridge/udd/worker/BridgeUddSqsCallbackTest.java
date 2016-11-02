package org.sagebionetworks.bridge.udd.worker;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.schema.UploadSchema;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.accounts.StormpathHelper;
import org.sagebionetworks.bridge.udd.dynamodb.DynamoHelper;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.helper.SesHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.synapse.SynapsePackager;

public class BridgeUddSqsCallbackTest {
    public BridgeUddSqsCallbackTest() throws IOException {
    }

    // mock objects - These are used only as passthroughs between the sub-components. So just create mocks instead
    // of instantiating all the fields.
    private static final StudyInfo MOCK_STUDY_INFO = mock(StudyInfo.class);
    private static final Map<String, UploadSchema> MOCK_SYNAPSE_TO_SCHEMA = ImmutableMap.of();
    private static final Set<String> MOCK_SURVEY_TABLE_ID_SET = ImmutableSet.of();
    private static final PresignedUrlInfo MOCK_PRESIGNED_URL_INFO = mock(PresignedUrlInfo.class);

    // simple strings for test
    private static final String EMAIL = "test@example.com";
    private static final String HEALTH_CODE = "test-health-code";
    private static final String HEALTH_ID = "test-health-id";
    private static final String STUDY_ID = "test-study";

    // non-mock test objects - We break inside these objects to get data.
    private static final AccountInfo ACCOUNT_INFO = new AccountInfo.Builder().withEmailAddress(EMAIL)
            .withHealthId(HEALTH_ID).withUsername(EMAIL).build();

    // test request
    private static final String REQUEST_JSON_TEXT = "{\n" +
            "   \"studyId\":\"" + STUDY_ID +"\",\n" +
            "   \"username\":\"" + EMAIL + "\",\n" +
            "   \"startDate\":\"2015-03-09\",\n" +
            "   \"endDate\":\"2015-03-31\"\n" +
            "}";

    private final JsonNode REQUEST_JSON = DefaultObjectMapper.INSTANCE.readValue(REQUEST_JSON_TEXT, JsonNode.class);

    private static final String Invalid_JSON_TEXT = "{\n" +
            "   \"invalidType\":\"" + STUDY_ID +"\",\n" +
            "   \"username\":\"" + EMAIL + "\",\n" +
            "   \"startDate\":\"2015-03-09\",\n" +
            "   \"endDate\":\"2015-03-31\"\n" +
            "}";

    private final JsonNode INVALID_REQUEST_JSON = DefaultObjectMapper.INSTANCE.readValue(Invalid_JSON_TEXT, JsonNode.class);



    // test members
    private BridgeUddSqsCallback callback;
    private SynapsePackager mockPackager;
    private SesHelper mockSesHelper;

    @BeforeMethod
    public void setup() throws Exception {
        // mock dynamo helper
        DynamoHelper mockDynamoHelper = mock(DynamoHelper.class);
        when(mockDynamoHelper.getStudy(STUDY_ID)).thenReturn(MOCK_STUDY_INFO);
        when(mockDynamoHelper.getHealthCodeFromHealthId(HEALTH_ID)).thenReturn(HEALTH_CODE);
        when(mockDynamoHelper.getSynapseTableIdsForStudy(STUDY_ID)).thenReturn(MOCK_SYNAPSE_TO_SCHEMA);
        when(mockDynamoHelper.getSynapseSurveyTablesForStudy(STUDY_ID)).thenReturn(MOCK_SURVEY_TABLE_ID_SET);

        // mock stormpath helper
        StormpathHelper mockStormpathHelper = mock(StormpathHelper.class);
        when(mockStormpathHelper.getAccount(same(MOCK_STUDY_INFO), eq(EMAIL))).thenReturn(ACCOUNT_INFO);

        // mock SES helper
        mockSesHelper = mock(SesHelper.class);

        // mock Synapse packager
        mockPackager = mock(SynapsePackager.class);

        // set up callback
        callback = new BridgeUddSqsCallback();
        callback.setDynamoHelper(mockDynamoHelper);
        callback.setSesHelper(mockSesHelper);
        callback.setStormpathHelper(mockStormpathHelper);
        callback.setSynapsePackager(mockPackager);
    }

    @Test
    public void noData() throws Exception {
        // set up packager call
        when(mockPackager.packageSynapseData(same(MOCK_SYNAPSE_TO_SCHEMA), eq(HEALTH_CODE),
                any(BridgeUddRequest.class), same(MOCK_SURVEY_TABLE_ID_SET))).thenReturn(null);

        // execute
        callback.callback(REQUEST_JSON);

        // verify SesHelper calls
        verify(mockSesHelper).sendNoDataMessageToAccount(same(MOCK_STUDY_INFO), same(ACCOUNT_INFO));
        verifyNoMoreInteractions(mockSesHelper);
    }

    @Test
    public void sendPresignedUrl() throws Exception {
        // set up packager call
        when(mockPackager.packageSynapseData(same(MOCK_SYNAPSE_TO_SCHEMA), eq(HEALTH_CODE),
                any(BridgeUddRequest.class), same(MOCK_SURVEY_TABLE_ID_SET))).thenReturn(MOCK_PRESIGNED_URL_INFO);

        // execute
        callback.callback(REQUEST_JSON);

        // verify SesHelper calls
        verify(mockSesHelper).sendPresignedUrlToAccount(same(MOCK_STUDY_INFO), same(MOCK_PRESIGNED_URL_INFO),
                same(ACCOUNT_INFO));
        verifyNoMoreInteractions(mockSesHelper);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void malformedRequest() throws Exception {
        callback.callback(INVALID_REQUEST_JSON);
    }
}
