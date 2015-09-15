package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.repo.model.file.BulkFileDownloadRequest;
import org.sagebionetworks.repo.model.file.BulkFileDownloadResponse;
import org.sagebionetworks.repo.model.file.FileHandleAssociateType;
import org.sagebionetworks.repo.model.file.FileHandleAssociation;
import org.sagebionetworks.repo.model.table.DownloadFromTableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.exceptions.AsyncTimeoutException;

/** Encapsulates calls to Synapse. */
@Component
public class SynapseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SynapseHelper.class);

    private int pollIntervalMillis;
    private int pollMaxTries;
    private SynapseClient synapseClient;

    /** Bridge config. This is used to get poll intervals and retry timeouts. */
    @Autowired
    public void setConfig(Config config) {
        pollIntervalMillis = config.getInt("synapse.poll.interval.millis");
        pollMaxTries = config.getInt("synapse.poll.max.tries");
    }

    /** Synapse client. */
    @Autowired
    public final void setSynapseClient(SynapseClient synapseClient) {
        this.synapseClient = synapseClient;
    }

    // TODO doc
    // TODO retries
    public void downloadFileHandle(String fileHandleId, File targetFile) throws SynapseException {
        synapseClient.downloadFromFileHandleTemporaryUrl(fileHandleId, targetFile);
    }

    // TODO doc
    // TODO retries
    public BulkFileDownloadResponse generateBulkDownloadFileHandle(String synapseTableId, Set<String> fileHandleIdSet)
            throws AsyncTimeoutException, SynapseException {
        // Need to create file handle association objects as part of the request.
        List<FileHandleAssociation> fhaList = new ArrayList<>();
        for (String oneFileHandleId : fileHandleIdSet) {
            FileHandleAssociation fha = new FileHandleAssociation();
            fha.setAssociateObjectId(synapseTableId);
            fha.setAssociateObjectType(FileHandleAssociateType.TableEntity);
            fha.setFileHandleId(oneFileHandleId);
            fhaList.add(fha);
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

    // TODO doc
    // TODO retries
    public String generateFileHandleFromTableQuery(String query, String synapseTableId) throws AsyncTimeoutException,
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
}
