package org.sagebionetworks.bridge.udd.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.springframework.stereotype.Component;

/** Helper class to wrap some Dynamo DB queries we make. */
@Component
public class DynamoHelper {
    private Table ddbHealthIdTable;
    private Table ddbStudyTable;
    private Table ddbSynapseMapTable;
    private Table ddbUploadSchemaTable;
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

    /** Upload schema table. */
    @Resource(name = "ddbUploadSchemaTable")
    public final void setDdbUploadSchemaTable(Table ddbUploadSchemaTable) {
        this.ddbUploadSchemaTable = ddbUploadSchemaTable;
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
     * IDs to the Bridge upload schemas.
     *
     * @param studyId
     *         ID of the study to query on
     * @return map from the Synapse table IDs to the Bridge upload schema keys
     */
    public Map<String, UploadSchema> getSynapseTableIdsForStudy(String studyId) throws IOException {
        // query and iterate
        List<UploadSchema> schemaList = new ArrayList<>();
        Iterable<Item> schemaItemIter = queryHelper(ddbUploadSchemaStudyIndex, "studyId", studyId, null);
        for (Item oneSchemaItem : schemaItemIter) {
            // Index only contains study ID, key, and revision. Re-query the table to get all fields.
            String key = oneSchemaItem.getString("key");
            int rev = oneSchemaItem.getInt("revision");
            Item fullSchemaItem = ddbUploadSchemaTable.getItem("key", key, "revision", rev);

            UploadSchema schema = UploadSchema.fromDdbItem(fullSchemaItem);
            schemaList.add(schema);
        }

        // Now query the SynapseTables table to get the Synapse table IDs for the schema. We use a reverse map from
        // Synapse table ID to upload schema, because multiple upload schemas can map to a single Synapse table. (This
        // is due to some early day hacks in the original studies.)
        Multimap<String, UploadSchema> synapseToSchemaMultimap = HashMultimap.create();
        for (UploadSchema oneSchema : schemaList) {
            Item synapseMapRecord = ddbSynapseMapTable.getItem("schemaKey", oneSchema.getKey().toString());
            if (synapseMapRecord == null) {
                // This could happen if the schema was just created, but the Bridge-Exporter hasn't created the
                // corresponding Synapse table yet. If so, there's obviously no data. Skip this one.
                continue;
            }

            String synapseTableId = synapseMapRecord.getString("tableId");
            synapseToSchemaMultimap.put(synapseTableId, oneSchema);
        }

        // Dedupe the upload schemas. We pick the canonical schema based on which one has the highest rev.
        Map<String, UploadSchema> synapseToSchemaMap = new HashMap<>();
        for (String oneSynapseTableId : synapseToSchemaMultimap.keySet()) {
            Iterable<UploadSchema> schemaIter = synapseToSchemaMultimap.get(oneSynapseTableId);
            UploadSchema canonicalSchema = null;
            for (UploadSchema oneSchema : schemaIter) {
                if (canonicalSchema == null ||
                        canonicalSchema.getKey().getRevision() < oneSchema.getKey().getRevision()) {
                    canonicalSchema = oneSchema;
                }
            }

            // Because of the way this code is written, there will always be at least one schema for this table ID, so
            // by this point, canonicalSchema won't be null.
            synapseToSchemaMap.put(oneSynapseTableId, canonicalSchema);
        }

        return synapseToSchemaMap;
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
