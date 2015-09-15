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
}
