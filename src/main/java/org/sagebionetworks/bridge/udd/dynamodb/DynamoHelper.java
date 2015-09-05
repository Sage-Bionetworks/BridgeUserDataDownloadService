package org.sagebionetworks.bridge.udd.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

/** Helper class to wrap some Dynamo DB queries we make. */
@Component
public class DynamoHelper {
    private Table ddbHealthIdTable;
    private Table ddbStudyTable;
    private Table ddbSynapseMapTable;
    private Index ddbUploadTableIndex;
    private Index ddbUploadSchemaStudyIndex;

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

    /** DDB table that maps upload schemas to Synapse table IDs. */
    @Resource(name = "ddbSynapseMapTable")
    public final void setDdbSynapseMapTable(Table ddbSynapseMapTable) {
        this.ddbSynapseMapTable = ddbSynapseMapTable;
    }

    /** Upload table healthCode-uploadDate-index. */
    @Resource(name = "ddbUploadTableIndex")
    public final void setDdbUploadTableIndex(Index ddbUploadTableIndex) {
        this.ddbUploadTableIndex = ddbUploadTableIndex;
    }

    /** UploadSchema studyId-index. */
    @Resource(name = "ddbUploadSchemaStudyIndex")
    public final void setDdbUploadSchemaStudyIndex(Index ddbUploadSchemaStudyIndex) {
        this.ddbUploadSchemaStudyIndex = ddbUploadSchemaStudyIndex;
    }

    /**
     * Gets the user's health code from their health ID.
     *
     * @param healthId
     *         user's health ID
     * @return user's health code
     */
    public String getHealthCodeFromHealthId(String healthId) {
        // convert healthId to healthCode
        Item healthIdItem = ddbHealthIdTable.getItem("id", healthId);
        return healthIdItem.getString("code");
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
     * Gets the Synapse table IDs associated with this study. The results are returned as a map from the Synapse table
     * IDs to the Bridge upload schema keys.
     *
     * @param studyId
     *         ID of the study to query on
     * @return map from the Synapse table IDs to the Bridge upload schema keys
     */
    public Map<String, UploadSchemaKey> getSynapseTableIdsForStudy(String studyId) {
        // The DDB key for upload schemas is in the form "[studyId]:[schemaId]", so strip away the "[studyId]:" part.
        // The prefix length is the length of studyId + 1 (for the colon).
        int studyPrefixLen = studyId.length() + 1;

        // query and iterate
        Set<UploadSchemaKey> schemaSet = new HashSet<>();
        Iterable<Item> schemaIter = queryHelper(ddbUploadSchemaStudyIndex, "studyId", studyId, null);
        for (Item oneSchema : schemaIter) {
            String ddbKey = oneSchema.getString("key");
            String schemaId = ddbKey.substring(studyPrefixLen);
            int rev = oneSchema.getInt("revision");
            UploadSchemaKey schemaKey = new UploadSchemaKey.Builder().withStudyId(studyId).withSchemaId(schemaId)
                    .withRevision(rev).build();
            schemaSet.add(schemaKey);
        }

        // Now query the SynapseTables table to get the Synapse table IDs for the schema. We use a reverse map from
        // Synapse table ID to upload schema, because multiple upload schemas can map to a single Synapse table. (This
        // is due to some early day hacks in the original studies.)
        Multimap<String, UploadSchemaKey> synapseToSchemaMultimap = HashMultimap.create();
        for (UploadSchemaKey oneSchemaKey : schemaSet) {
            Item synapseMapRecord = ddbSynapseMapTable.getItem("schemaKey", oneSchemaKey.toString());
            String synapseTableId = synapseMapRecord.getString("tableId");
            synapseToSchemaMultimap.put(synapseTableId, oneSchemaKey);
        }

        // Dedupe the upload schema keys. We pick the canonical schema based on which one has the highest rev.
        Map<String, UploadSchemaKey> synapseToSchemaMap = new HashMap<>();
        for (String oneSynapseTableId : synapseToSchemaMultimap.keySet()) {
            Iterable<UploadSchemaKey> schemaKeyIter = synapseToSchemaMultimap.get(oneSynapseTableId);
            UploadSchemaKey canonicalSchemaKey = null;
            for (UploadSchemaKey oneSchemaKey : schemaKeyIter) {
                if (canonicalSchemaKey == null || canonicalSchemaKey.getRevision() < oneSchemaKey.getRevision()) {
                    canonicalSchemaKey = oneSchemaKey;
                }
            }

            // Because of the way this code is written, there will always be at least one schema for this table ID, so
            // by this point, canonicalSchemaKey won't be null.
            synapseToSchemaMap.put(oneSynapseTableId, canonicalSchemaKey);
        }

        return synapseToSchemaMap;
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
        String healthCode = getHealthCodeFromHealthId(accountInfo.getHealthId());

        // get uploads from healthCode-uploadDate-index
        Iterable<Item> uploadIter = queryHelper(ddbUploadTableIndex, "healthCode", healthCode,
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
     * <p>
     * This abstracts away the call to Index.query(), which returns an ItemCollection. While ItemCollection implements
     * Iterable, it overrides iterator() to return an IteratorSupport, which is not publicly exposed. This makes
     * Index.query() nearly impossible to mock. So we abstract it away into a method that we can mock.
     * </p>
     * <p>
     * rangeKeyCondition is optional. If not specified, this will only run the query on the hash index.
     * </p>
     */
    protected Iterable<Item> queryHelper(Index index, String indexKeyName, Object indexKeyValue,
            RangeKeyCondition rangeKeyCondition) {
        if (rangeKeyCondition != null) {
            return index.query(indexKeyName, indexKeyValue, rangeKeyCondition);
        } else {
            return index.query(indexKeyName, indexKeyValue);
        }
    }
}
