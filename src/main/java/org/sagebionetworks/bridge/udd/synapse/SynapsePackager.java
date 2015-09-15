package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Resource;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.helper.FileHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

/**
 * Helper to query Synapse, download the results, and upload the results to S3 as a pre-signed URL. This acts as a
 * singular class with a bunch of its own helpers because (a) it needs multi-threading to query Synapse tables in
 * parallel and (b) it encapsulates all file system operations (through FileHelper).
 */
@Component
public class SynapsePackager {
    private static final Logger LOG = LoggerFactory.getLogger(SynapsePackager.class);

    private ExecutorService auxiliaryExecutorService;
    private FileHelper fileHelper;
    private SynapseHelper synapseHelper;

    /**
     * Auxiliary executor service (thread pool), used secondary thread tasks. (As opposed to listener executor service.
     */
    @Resource(name = "auxiliaryExecutorService")
    public final void setAuxiliaryExecutorService(ExecutorService auxiliaryExecutorService) {
        this.auxiliaryExecutorService = auxiliaryExecutorService;
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Synapse helper. */
    @Autowired
    public void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /**
     * Downloads data from Synapse tables, uploads them to S3, and generates a pre-signed URL for the data.
     *
     * @param synapseToSchemaMap
     *         map from Synapse table IDs to schemas, used to enumerate Synapse tables and determine file names
     * @param healthCode
     *         user health code to filter on
     * @param request
     *         user data download request, used to determine start and end dates for requested data
     * @return pre-signed URL and expiration time
     */
    public PresignedUrlInfo packageSynapseData(Map<String, UploadSchema> synapseToSchemaMap, String healthCode,
            BridgeUddRequest request) {
        LocalDate startDate = request.getStartDate();
        LocalDate endDate = request.getEndDate();

        // TODO user error log

        // create async threads to download CSVs
        File tmpDir = fileHelper.createTempDir();
        List<Future<List<File>>> taskFutureList = new ArrayList<>();
        for (Map.Entry<String, UploadSchema> oneSynapseToSchemaEntry : synapseToSchemaMap.entrySet()) {
            // create params
            String synapseTableId = oneSynapseToSchemaEntry.getKey();
            UploadSchema schema = oneSynapseToSchemaEntry.getValue();
            SynapseDownloadFromTableParameters param = new SynapseDownloadFromTableParameters.Builder()
                    .withSynapseTableId(synapseTableId).withHealthCode(healthCode).withStartDate(startDate)
                    .withEndDate(endDate).withTempDir(tmpDir).withSchema(schema).build();

            // kick off async task
            SynapseDownloadFromTableTask downloadCsvTask = newDownloadCsvTask(param);
            Future<List<File>> downloadCsvTaskFuture = auxiliaryExecutorService.submit(downloadCsvTask);
            taskFutureList.add(downloadCsvTaskFuture);
        }

        // join on threads until they're all done
        List<File> allFileList = new ArrayList<>();
        for (Future<List<File>> oneTaskFuture : taskFutureList) {
            List<File> taskFileList;
            try {
                taskFileList = oneTaskFuture.get();
            } catch (ExecutionException | InterruptedException ex) {
                LOG.error("Error downloading CSV: " + ex.getMessage(), ex);
                continue;
            }

            allFileList.addAll(taskFileList);
        }

        // TODO zip

        // TODO
        // TODO cleanup files
        return null;
    }

    // Creates a SynapseDownloadCsvFromTableTask. This is a member method, so we can mock out task execution in unit
    // tests. This is package-scoped to make it available to unit tests.
    //
    // We use a factory method here instead of using Spring, because Spring can't be mocked and we need to create
    // multiple copies of the synapse task.
    SynapseDownloadFromTableTask newDownloadCsvTask(SynapseDownloadFromTableParameters param) {
        SynapseDownloadFromTableTask task = new SynapseDownloadFromTableTask(param);
        task.setFileHelper(fileHelper);
        task.setSynapseHelper(synapseHelper);
        return task;
    }
}
