package org.sagebionetworks.bridge.udd.dynamodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyConditions;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

public class DynamoHelperTest {
    @Test
    public void testGetStudy() {
        // mock study table
        Item mockItem = new Item().withString("name", "Test Study").withString("stormpathHref", "dummy-stormpath-href")
                .withString("supportEmail", "support@sagebase.org");
        Table mockStudyTable = mock(Table.class);
        when(mockStudyTable.getItem("identifier", "test-study")).thenReturn(mockItem);

        // set up dynamo helper
        DynamoHelper dynamoHelper = new DynamoHelper();
        dynamoHelper.setDdbStudyTable(mockStudyTable);

        // execute and validate
        StudyInfo studyInfo = dynamoHelper.getStudy("test-study");
        assertEquals(studyInfo.getStudyId(), "test-study");
        assertEquals(studyInfo.getName(), "Test Study");
        assertEquals(studyInfo.getStormpathHref(), "dummy-stormpath-href");
        assertEquals(studyInfo.getSupportEmail(), "support@sagebase.org");
    }

    @Test
    public void testGetUploads() {
        // mock upload index
        List<Item> uploadList = new ArrayList<>();
        uploadList.add(new Item().withString("uploadId", "foo-upload").withString("uploadDate", "2015-08-15"));
        uploadList.add(new Item().withString("uploadId", "bar-upload").withString("uploadDate", "2015-08-17"));
        uploadList.add(new Item().withString("uploadId", "baz-upload").withString("uploadDate", "2015-08-19"));

        // Index.query() can't be mocked, so override queryHelper to sidestep this problem
        DynamoHelper testHelper = new DynamoHelper() {
            @Override
            protected Iterable<Item> queryHelper(Index index, String indexKeyName, Object indexKeyValue,
                    RangeKeyCondition rangeKeyCondition) {
                // TODO
                assertEquals(indexKeyName, "healthCode");
                assertEquals(indexKeyValue, "dummy-health-code");
                assertEquals(rangeKeyCondition.getAttrName(), "uploadDate");
                assertEquals(rangeKeyCondition.getKeyCondition(), KeyConditions.BETWEEN);
                assertEquals(rangeKeyCondition.getValues()[0], "2015-08-15");
                assertEquals(rangeKeyCondition.getValues()[1], "2015-08-19");
                return uploadList;
            }
        };

        // mock health ID table
        Item mockHealthIdItem = new Item().withString("code", "dummy-health-code");
        Table mockHealthIdTable = mock(Table.class);
        when(mockHealthIdTable.getItem("id", "dummy-health-id")).thenReturn(mockHealthIdItem);
        testHelper.setDdbHealthIdTable(mockHealthIdTable);

        // set up inputs
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthId("dummy-health-id").withUsername("dummy-username").build();
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId("test-study")
                .withUsername("dummy-username").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();

        // execute and validate
        List<UploadInfo> uploadInfoList = testHelper.getUploadsForRequest(accountInfo, request);
        assertEquals(uploadInfoList.size(), 3);

        assertEquals(uploadInfoList.get(0).getId(), "foo-upload");
        assertEquals(uploadInfoList.get(0).getUploadDate().toString(), "2015-08-15");

        assertEquals(uploadInfoList.get(1).getId(), "bar-upload");
        assertEquals(uploadInfoList.get(1).getUploadDate().toString(), "2015-08-17");

        assertEquals(uploadInfoList.get(2).getId(), "baz-upload");
        assertEquals(uploadInfoList.get(2).getUploadDate().toString(), "2015-08-19");
    }
}
