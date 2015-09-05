package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.Map;

import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.dynamodb.UploadSchemaKey;
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

    private Config config;
    private FileHelper fileHelper;
    private SynapseClient synapseClient;

    /** Bridge config. This is used to get poll intervals and retry timeouts. */
    @Autowired
    public final void setConfig(Config config) {
        this.config = config;
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Synapse client. */
    @Autowired
    public final void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    // TODO document
    public PresignedUrlInfo packageSynapseData(Map<String, UploadSchemaKey> synapseToSchemaMap, String healthCode,
            BridgeUddRequest request) {
        int pollMaxTries = config.getInt("synapse.poll.max.tries");
        int pollIntervalMillis = config.getInt("synapse.poll.interval.millis");
        LocalDate startDate = request.getStartDate();
        LocalDate endDate = request.getEndDate();

        // TODO multithreading
        // TODO timing logging
        // TODO if the CSV has only 1 row (headers), don't package it
        File tmpDir = fileHelper.createTempDir();
        for (Map.Entry<String, UploadSchemaKey> oneSynapseToSchemaEntry : synapseToSchemaMap.entrySet()) {
            // filenames are named after the upload schema
            String synapseTableId = oneSynapseToSchemaEntry.getKey();
            UploadSchemaKey schemaKey = oneSynapseToSchemaEntry.getValue();
            File csvFile = fileHelper.newFile(tmpDir, schemaKey.toString() + ".csv");
            SynapseDownloadCsvTask downloadCsvTask = newDownloadCsvTask(synapseClient, pollMaxTries,
                    pollIntervalMillis, synapseTableId, healthCode, startDate, endDate, csvFile);
            downloadCsvTask.run();

            LOG.info("Downloaded CSV file " + csvFile.getAbsolutePath());
        }

        // TODO
        return null;
    }

    // Creates a SynapseDownloadCsvFromTableTask. This is a member method, so we can mock out task execution in unit
    // tests. This is package-scoped to make it available to unit tests.
    SynapseDownloadCsvTask newDownloadCsvTask(SynapseClient synapseClient, int pollMaxTries, int pollIntervalMillis,
            String synapseTableId, String healthCode, LocalDate startDate, LocalDate endDate, File targetFile) {
        return new SynapseDownloadCsvTask(synapseClient, pollMaxTries, pollIntervalMillis, synapseTableId,
                healthCode, startDate, endDate, targetFile);
    }
}
