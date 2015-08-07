package org.sagebionetworks.bridge.udd;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.google.common.base.Joiner;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class BridgeUddDynamoExporter {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeUddDynamoExporter.class);

    private Index ddbUploadTableIndex;

    @Resource(name = "ddbUploadTableIndex")
    public void setDdbUploadTableIndex(Index ddbUploadTableIndex) {
        this.ddbUploadTableIndex = ddbUploadTableIndex;
    }

    public void exportForRequest(BridgeUddRequest request) {
        String startDateString = request.getStartDate().toString(ISODateTimeFormat.date());
        String endDateString = request.getEndDate().toString(ISODateTimeFormat.date());

        // TODO: add this compound index to the BridgePF codebase
        Iterable<Item> uploadIter = ddbUploadTableIndex.query("healthCode", request.getHealthCode(),
                new RangeKeyCondition("uploadDate").between(startDateString, endDateString));

        List<String> uploadIdList = new ArrayList<>();
        for (Item oneUpload : uploadIter) {
            uploadIdList.add(oneUpload.getString("uploadId"));
        }

        LOG.info("Found upload IDs: " + Joiner.on(", ").join(uploadIdList));
    }
}
