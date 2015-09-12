package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadRequest;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociation;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;
import org.sagebionetworks.util.csv.CsvNullReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTaskExecutionException;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;
import org.sagebionetworks.bridge.udd.helper.FileHelper;

/**
 * A one-shot asynchronous task to query a Synapse table and download the CSV. This task returns the list of files
 * downloaded. This includes the CSV (if the query pulls data from the table) and a ZIP with the attached file handles
 * (if there are any).
 */
public class SynapseDownloadFromTableTask implements Callable<List<File>> {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseDownloadFromTableTask.class);

    private static final String QUERY_TEMPLATE =
            "SELECT * FROM %s WHERE healthCode = '%s' AND uploadDate >= '%s' AND uploadDate <= '%s'";

    // Task parameters. Passed in by constructor.
    private final SynapseDownloadFromTableParameters param;

    // Helpers and config objects. Originates from Spring configs and is passed in threw setters using a similar
    // pattern.
    private FileHelper fileHelper;
    private int pollIntervalMillis;
    private int pollMaxTries;
    private SynapseClient synapseClient;

    /**
     * Constructs this task with the specified task parameters
     * @param param task parameters
     */
    public SynapseDownloadFromTableTask(SynapseDownloadFromTableParameters param) {
        this.param = param;
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    public void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** Interval between each Synapse poll attempt. */
    public void setPollIntervalMillis(int pollIntervalMillis) {
        this.pollIntervalMillis = pollIntervalMillis;
    }

    /** Number of tries to poll Synapse before timing out the async request. */
    public void setPollMaxTries(int pollMaxTries) {
        this.pollMaxTries = pollMaxTries;
    }

    /** Synapse client. */
    public void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    @Override
    public List<File> call() {
        // unpack params
        String synapseTableId = param.getSynapseTableId();
        String healthCode = param.getHealthCode();
        LocalDate startDate = param.getStartDate();
        LocalDate endDate = param.getEndDate();
        File tmpDir = param.getTempDir();
        UploadSchema schema = param.getSchema();
        String schemaKeyStr = schema.getKey().toString();

        // Download CSV from table query.
        String csvFileName = schemaKeyStr + ".csv";
        File csvFile = fileHelper.newFile(tmpDir, csvFileName);
        String csvFilePath = csvFile.getAbsolutePath();
        Stopwatch downloadCsvStopwatch = Stopwatch.createStarted();
        try {
            String query = String.format(QUERY_TEMPLATE, synapseTableId, healthCode, startDate, endDate);
            String csvFileHandleId = generateFileHandleFromTableQuery(query, synapseTableId);

            // TODO: add retries to this call
            synapseClient.downloadFromFileHandleTemporaryUrl(csvFileHandleId, csvFile);
        } catch (AsyncTimeoutException | SynapseException ex) {
            throw new AsyncTaskExecutionException("Error downloading synapse table " + synapseTableId + " to file " +
                    csvFilePath + ": " + ex.getMessage(), ex);
        } finally {
            downloadCsvStopwatch.stop();
            LOG.info("Downloading from synapse table " + synapseTableId + " to file " + csvFilePath + " took " +
                    downloadCsvStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }

        // Count file lines. If there aren't at least 2 lines (1 line headers, 1+ lines of data), then filter out the
        // file, since we don't need it. Don't log or throw, since this could be a normal case.
        try {
            List<String> lineList = fileHelper.readLines(csvFile);
            if (lineList.size() < 2) {
                LOG.info("No user data found for file " + csvFilePath + ". Short-circuiting.");

                // TODO delete the file, since it's no longer needed

                // return an empty list, to signify no data
                return ImmutableList.of();
            }
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error counting lines for file " + csvFilePath + ": " +
                    ex.getMessage(), ex);
        }

        // extract file handles
        Stopwatch extractFileHandlesStopwatch = Stopwatch.createStarted();
        Set<String> fileHandleIdSet;
        try {
            fileHandleIdSet = extractFileHandleIdsFromCsv(schema, csvFile);
        } catch (IOException ex) {
            throw new AsyncTaskExecutionException("Error extracting file handle IDs from file " + csvFilePath + ": " +
                    ex.getMessage(), ex);
        } finally {
            extractFileHandlesStopwatch.stop();
            LOG.info("Extracting file handle IDs from file " + csvFilePath + " took " +
                    extractFileHandlesStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }

        // If there are no file handles to download, short-circuit. Return a list with just the CSV file, since that's
        // all we need.
        if (fileHandleIdSet.isEmpty()) {
            LOG.info("No file handles to download for file " + csvFilePath + ". Short-circuiting.");
            return ImmutableList.of(csvFile);
        }

        // download file handles
        String bulkDownloadFileName = schemaKeyStr + ".csv";
        File bulkDownloadFile = fileHelper.newFile(tmpDir, bulkDownloadFileName);
        String bulkDownloadFilePath = bulkDownloadFile.getAbsolutePath();
        Stopwatch bulkDownloadStopwatch = Stopwatch.createStarted();
        BulkFileDownloadResponse bulkDownloadResponse;
        try {
            bulkDownloadResponse = bulkDownloadFileHandles(synapseTableId, fileHandleIdSet);
            String bulkDownloadFileHandleId = bulkDownloadResponse.getResultZipFileHandleId();

            // TODO: add retries to this call
            synapseClient.downloadFromFileHandleTemporaryUrl(bulkDownloadFileHandleId, bulkDownloadFile);
        } catch (AsyncTimeoutException | SynapseException ex) {
            throw new AsyncTaskExecutionException("Error bulk downloading file handles to file " +
                    bulkDownloadFilePath + ": " + ex.getMessage(), ex);
        } finally {
            bulkDownloadStopwatch.stop();
            LOG.info("Bulk downloading file handles to file " + bulkDownloadFilePath + " took " +
                    bulkDownloadStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }

        // TODO: transform CSV to have zip-relative paths instead of file handle IDs
        // TODO: file cleanup
    }

    // TODO doc
    // TODO retries
    private String generateFileHandleFromTableQuery(String query, String synapseTableId) throws AsyncTimeoutException,
            SynapseException {
        // Kick off async call.
        String asyncJobToken = synapseClient.downloadCsvFromTableAsyncStart(query, /*writeHeader*/true,
                    /*includeRowIdAndRowVersion*/false, /*csvDescriptor*/null, synapseTableId);

        // Poll Synapse until results are ready.
        DownloadFromTableResult result = null;
        for (int tries = 0; tries < pollMaxTries; tries++) {
            if (pollIntervalMillis > 0) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
            }

            try {
                result = synapseClient.downloadCsvFromTableAsyncGet(asyncJobToken, synapseTableId);
                break;
            } catch (SynapseResultNotReadyException ex) {
                // Result not ready. Spin around one more time.
            }
        }

        if (result == null) {
            throw new AsyncTimeoutException("Download CSV returned null results for table " + synapseTableId);
        }
        return result.getResultsFileHandleId();
    }

    // TODO doc
    private Set<String> extractFileHandleIdsFromCsv(UploadSchema schema, File csvFile) throws IOException {
        Map<String, String> fieldTypeMap = schema.getFieldTypeMap();

        try (CsvNullReader csvFileReader = new CsvNullReader(fileHelper.getReader(csvFile))) {
            // Get first row, the header row. Because of our previous check, we know this row must exist.
            String[] headerRow = csvFileReader.readNext();

            // Iterate through the headers. Identify the ones that are file handles.
            Set<Integer> fileHandleColIdxSet = new HashSet<>();
            for (int i = 0; i < headerRow.length; i++) {
                String oneFieldName = headerRow[i];
                String bridgeType = fieldTypeMap.get(oneFieldName);
                if (bridgeType != null && UploadSchema.ATTACHMENT_TYPE_SET.contains(bridgeType)) {
                    fileHandleColIdxSet.add(i);
                }
            }

            // Shortcut: If the file handle column set is empty, there are no file handles, so we can skip.
            if (fileHandleColIdxSet.isEmpty()) {
                return ImmutableSet.of();
            }

            // Iterate through the rows. Using the col idx set, identify file handle IDs.
            Set<String> fileHandleIdSet = new HashSet<>();
            String[] row;
            while ((row = csvFileReader.readNext()) != null) {
                //noinspection Convert2streamapi
                for (int oneFileHandleColIdx : fileHandleColIdxSet) {
                    fileHandleIdSet.add(row[oneFileHandleColIdx]);
                }
            }

            return fileHandleIdSet;
        }
    }

    // TODO doc
    // TODO retries
    private BulkFileDownloadResponse bulkDownloadFileHandles(String synapseTableId, Set<String> fileHandleIdSet)
            throws AsyncTimeoutException, SynapseException {
        // Need to create file handle association objects as part of the request.
        List<FileHandleAssociation> fhaList = new ArrayList<>();
        for (String oneFileHandleId : fileHandleIdSet) {
            FileHandleAssociation fha = new FileHandleAssociation();
            fha.setAssociateObjectId(synapseTableId);
            fha.setAssociateObjectType(FileHandleAssociateType.TableEntity);
            fha.setFileHandleId(oneFileHandleId);
        }

        // create request
        BulkFileDownloadRequest request = new BulkFileDownloadRequest();
        request.setRequestedFiles(fhaList);

        // Kick off async call.
        String asyncJobToken = synapseClient.startBulkFileDownload(request);

        // Poll Synapse until results are ready.
        BulkFileDownloadResponse response = null;
        for (int tries = 0; tries < pollMaxTries; tries++) {
            if (pollIntervalMillis > 0) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
            }

            try {
                response = synapseClient.getBulkFileDownloadResults(asyncJobToken);
                break;
            } catch (SynapseResultNotReadyException ex) {
                // Result not ready. Spin around one more time.
            }
        }

        if (response == null) {
            throw new AsyncTimeoutException("Bulk file download returned null result for table " + synapseTableId);
        }
        return response;
    }
}
