package org.sagebionetworks.bridge.udd.dynamodb;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

@Component
public class DynamoHelper {
    private Table ddbHealthIdTable;
    private Table ddbStudyTable;
    private Index ddbUploadTableIndex;

    @Resource(name = "ddbHealthIdTable")
    public final void setDdbHealthIdTable(Table ddbHealthIdTable) {
        this.ddbHealthIdTable = ddbHealthIdTable;
    }

    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    @Resource(name = "ddbUploadTableIndex")
    public final void setDdbUploadTableIndex(Index ddbUploadTableIndex) {
        this.ddbUploadTableIndex = ddbUploadTableIndex;
    }

    public StudyInfo getStudy(String studyId) {
        Item study = ddbStudyTable.getItem("identifier", studyId);
        return new StudyInfo.Builder().withStudyId(studyId).withStormpathHref(study.getString("stormpathHref"))
                .build();
    }

    public List<UploadInfo> getUploadsForRequest(AccountInfo accountInfo, BridgeUddRequest request) {
        String startDateString = request.getStartDate().toString(ISODateTimeFormat.date());
        String endDateString = request.getEndDate().toString(ISODateTimeFormat.date());

        // convert healthId to healthCode
        Item healthIdItem = ddbHealthIdTable.getItem("id", accountInfo.getHealthId());
        String healthCode = healthIdItem.getString("code");

        // TODO: add this compound index to the BridgePF codebase
        Iterable<Item> uploadIter = ddbUploadTableIndex.query("healthCode", healthCode,
                new RangeKeyCondition("uploadDate").between(startDateString, endDateString));

        List<UploadInfo> uploadInfoList = new ArrayList<>();
        for (Item oneUpload : uploadIter) {
            String uploadId = oneUpload.getString("uploadId");
            String uploadDateStr = oneUpload.getString("uploadDate");
            UploadInfo oneUploadInfo = new UploadInfo.Builder().withId(uploadId)
                    .withUploadDate(uploadDateStr).build();
            uploadInfoList.add(oneUploadInfo);
        }
        return uploadInfoList;
    }
}
