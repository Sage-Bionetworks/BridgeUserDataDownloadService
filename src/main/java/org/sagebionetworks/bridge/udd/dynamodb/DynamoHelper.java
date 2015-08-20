package org.sagebionetworks.bridge.udd.dynamodb;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

/** Helper class to wrap some Dynamo DB queries we make. */
@Component
public class DynamoHelper {
    private Table ddbHealthIdTable;
    private Table ddbStudyTable;
    private Index ddbUploadTableIndex;

    /** Health ID table. */
    @Resource(name = "ddbHealthIdTable")
    public final void setDdbHealthIdTable(Table ddbHealthIdTable) {
        this.ddbHealthIdTable = ddbHealthIdTable;
    }

    /** Study table. */
    @Resource(name = "ddbStudyTable")
    public final void setDdbStudyTable(Table ddbStudyTable) {
        this.ddbStudyTable = ddbStudyTable;
    }

    /** Upload table healthCode-uploadDate-index. */
    @Resource(name = "ddbUploadTableIndex")
    public final void setDdbUploadTableIndex(Index ddbUploadTableIndex) {
        this.ddbUploadTableIndex = ddbUploadTableIndex;
    }

    /**
     * Gets study info for the given study ID.
     *
     * @param studyId
     *         ID of study to fetch
     * @return the requested study
     */
    public StudyInfo getStudy(String studyId) {
        Item study = ddbStudyTable.getItem("identifier", studyId);

        String studyName = study.getString("name");
        String stormpathHref = study.getString("stormpathHref");
        String supportEmail = study.getString("supportEmail");

        return new StudyInfo.Builder().withName(studyName).withStudyId(studyId).withStormpathHref(stormpathHref)
                .withSupportEmail(supportEmail).build();
    }

    /**
     * Fetches uploads for the given account matching the request parameters.
     *
     * @param accountInfo
     *         account info needed to complete the request
     * @param request
     *         request parameters to match uploads against
     * @return list of uploads matching the given account and request
     */
    public List<UploadInfo> getUploadsForRequest(AccountInfo accountInfo, BridgeUddRequest request) {
        String startDateString = request.getStartDate().toString();
        String endDateString = request.getEndDate().toString();

        // convert healthId to healthCode
        Item healthIdItem = ddbHealthIdTable.getItem("id", accountInfo.getHealthId());
        String healthCode = healthIdItem.getString("code");

        // get uploads from healthCode-uploadDate-index
        Iterable<Item> uploadIter = queryHelper("healthCode", healthCode,
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

    /**
     * This abstracts away the call to Index.query(), which returns an ItemCollection. While ItemCollection implements
     * Iterable, it overrides iterator() to return an IteratorSupport, which is not publicly exposed. This makes
     * Index.query() nearly impossible to mock. So we abstract it away into a method that we can mock.
     */
    protected Iterable<Item> queryHelper(String indexKeyName, Object indexKeyValue,
            RangeKeyCondition rangeKeyCondition) {
        return ddbUploadTableIndex.query(indexKeyName, indexKeyValue, rangeKeyCondition);
    }
}
