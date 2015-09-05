package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;

import org.joda.time.LocalDate;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sagebionetworks.bridge.udd.exceptions.AsyncTaskExecutionException;

/** A one-shot asynchronous task to query a Synapse table and download the CSV. */
public class SynapseDownloadCsvTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseDownloadCsvTask.class);

    private static final String QUERY_TEMPLATE =
            "SELECT * FROM %s WHERE healthCode = '%s' AND uploadDate >= '%s' AND uploadDate <= '%s'";

    private final SynapseClient synapseClient;
    private final int pollMaxTries;
    private final int pollIntervalMillis;
    private final String synapseTableId;
    private final String healthCode;
    private final LocalDate startDate;
    private final LocalDate endDate;
    private final File targetFile;

    /**
     * Constructs the download CSV from table task.
     *
     * @param synapseClient
     *         Synapse client, which we use to query Synapse with
     * @param pollMaxTries
     *         number of times we poll the asynchronous Synapse call
     * @param pollIntervalMillis
     *         time in milliseconds between each poll for the asynchronous Synapse call
     * @param synapseTableId
     *         ID of the Synapse table to query against
     * @param healthCode
     *         user health code to filter against
     * @param startDate
     *         start date to filter against
     * @param endDate
     *         end date to filter against
     * @param targetFile
     *         target file to download the CSV into
     */
    public SynapseDownloadCsvTask(SynapseClient synapseClient, int pollMaxTries, int pollIntervalMillis,
            String synapseTableId, String healthCode, LocalDate startDate, LocalDate endDate, File targetFile) {
        this.synapseClient = synapseClient;
        this.pollMaxTries = pollMaxTries;
        this.pollIntervalMillis = pollIntervalMillis;
        this.synapseTableId = synapseTableId;
        this.healthCode = healthCode;
        this.startDate = startDate;
        this.endDate = endDate;
        this.targetFile = targetFile;
    }

    @Override
    public void run() {
        // Query synapse. This is an asynchronous call.
        String query = String.format(QUERY_TEMPLATE, synapseTableId, healthCode, startDate, endDate);
        String asyncJobToken;
        try {
            asyncJobToken = synapseClient.downloadCsvFromTableAsyncStart(query, /*writeHeader*/true,
                    /*includeRowIdAndRowVersion*/false, /*csvDescriptor*/null, synapseTableId);
        } catch (SynapseException ex) {
            throw new AsyncTaskExecutionException("Download CSV Async Start failed for tableId=" + synapseTableId +
                    ": " + ex.getMessage(), ex);
        }

        // Poll synapse until results are ready.
        DownloadFromTableResult downloadFromTableResult = null;
        for (int tries = 0; tries < pollMaxTries; tries++) {
            if (pollIntervalMillis > 0) {
                try {
                    Thread.sleep(pollIntervalMillis);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping: " + ex.getMessage(), ex);
                }
            }

            try {
                downloadFromTableResult = synapseClient.downloadCsvFromTableAsyncGet(asyncJobToken, synapseTableId);
                break;
            } catch (SynapseResultNotReadyException ex) {
                // Result not ready. Spin around one more time.
            } catch (SynapseException ex) {
                throw new AsyncTaskExecutionException("Download CSV Async Get failed for tableId=" + synapseTableId +
                        ": " + ex.getMessage(), ex);
            }
        }
        if (downloadFromTableResult == null) {
            throw new AsyncTaskExecutionException("Download CSV returned null results for tableId=" + synapseTableId);
        }

        // DownloadFromTableResults has a reference to a file handle, where the actual results are. Download that file
        // handle.
        String fileHandleId = downloadFromTableResult.getResultsFileHandleId();
        try {
            synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, targetFile);
        } catch (SynapseException ex) {
            throw new AsyncTaskExecutionException("Download File Handle failed for fileHandleId=" + fileHandleId);
        }
    }
}
