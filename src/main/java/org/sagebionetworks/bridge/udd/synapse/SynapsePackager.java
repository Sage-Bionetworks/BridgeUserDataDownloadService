package org.sagebionetworks.bridge.udd.synapse;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import com.amazonaws.HttpMethod;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.helper.FileHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.s3.S3Helper;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

/**
 * Helper to query Synapse, download the results, and upload the results to S3 as a pre-signed URL. This acts as a
 * singular class with a bunch of its own helpers because (a) it needs multi-threading to query Synapse tables in
 * parallel and (b) it encapsulates all file system operations (through FileHelper).
 */
@Component
public class SynapsePackager {
    private static final Logger LOG = LoggerFactory.getLogger(SynapsePackager.class);

    // package-scoped to be available in tests
    static final String CONFIG_KEY_EXPIRATION_HOURS = "s3.url.expiration.hours";
    static final String CONFIG_KEY_USERDATA_BUCKET = "userdata.bucket";
    static final String ERROR_LOG_FILE_NAME = "error.log";

    private static final Joiner LINE_JOINER = Joiner.on('\n');

    private ExecutorService auxiliaryExecutorService;
    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private SynapseHelper synapseHelper;
    private int urlExpirationHours;
    private String userdataBucketName;
    private ZipHelper zipHelper;

    /**
     * Auxiliary executor service (thread pool), used secondary thread tasks. (As opposed to listener executor service.
     */
    @Resource(name = "auxiliaryExecutorService")
    public final void setAuxiliaryExecutorService(ExecutorService auxiliaryExecutorService) {
        this.auxiliaryExecutorService = auxiliaryExecutorService;
    }

    /** Bridge config, used to get the S3 upload bucket and pre-signed URL expiration. */
    @Autowired
    public final void setConfig(Config config) {
        urlExpirationHours = config.getInt(CONFIG_KEY_EXPIRATION_HOURS);
        userdataBucketName = config.get(CONFIG_KEY_USERDATA_BUCKET);
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** S3 Helper, used to upload to S3 and create a pre-signed URL. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** Synapse helper. */
    @Autowired
    public final void setSynapseHelper(SynapseHelper synapseHelper) {
        this.synapseHelper = synapseHelper;
    }

    /** Zip helper. */
    @Autowired
    public final void setZipHelper(ZipHelper zipHelper) {
        this.zipHelper = zipHelper;
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
            BridgeUddRequest request) throws IOException {
        List<File> allFileList = null;
        File masterZipFile = null;
        File tmpDir = fileHelper.createTempDir();
        try {
            // create and execute Synapse downloads asynchronously
            List<Future<SynapseDownloadFromTableResult>> taskFutureList = initAsyncTasks(synapseToSchemaMap,
                    healthCode, request, tmpDir);
            allFileList = waitForAsyncTasks(tmpDir, taskFutureList);

            if (allFileList.isEmpty()) {
                // There are no files to send, meaning there is no user data to send. Return null, to signal that there
                // is no pre-signed URL to send.
                return null;
            }

            // Zip up all upload files. Filename is "userdata-[startDate]-to-[endDate]-[random guid].zip". This allows
            // the filename to be unique, user-friendly, and contain no identifying info.
            String masterZipFileName = "userdata-" + request.getStartDate() + "-to-" + request.getEndDate() + "-" +
                    UUID.randomUUID().toString() + ".zip";
            masterZipFile = fileHelper.newFile(tmpDir, masterZipFileName);
            zipFiles(allFileList, masterZipFile);

            uploadToS3(masterZipFile);
            return generatePresignedUrlInfo(masterZipFileName);
        } finally {
            cleanupFiles(allFileList, masterZipFile, tmpDir);
        }
    }

    /**
     * <p>
     * Kicks off the async SynapseDownloadFromTableTasks. These tasks query the Synapse table and download both the
     * CSV and the bulk download for attached file handles.
     * </p>
     * <p>
     * This is made package-scoped so unit tests can hook into it.
     * </p>
     *
     * @param synapseToSchemaMap
     *         map of all Synapse table IDs in the current study and their corresponding schemas
     * @param healthCode
     *         user's health code, used for generating queries
     * @param request
     *         Bridge UDD request, used to get the start and end date
     * @param tmpDir
     *         temp directory that files should be downloaded to
     * @return list of Futures for the async tasks
     */
    List<Future<SynapseDownloadFromTableResult>> initAsyncTasks(Map<String, UploadSchema> synapseToSchemaMap,
            String healthCode, BridgeUddRequest request, File tmpDir) {
        List<Future<SynapseDownloadFromTableResult>> taskFutureList = new ArrayList<>();
        for (Map.Entry<String, UploadSchema> oneSynapseToSchemaEntry : synapseToSchemaMap.entrySet()) {
            // create params
            String synapseTableId = oneSynapseToSchemaEntry.getKey();
            UploadSchema schema = oneSynapseToSchemaEntry.getValue();
            SynapseDownloadFromTableParameters param = new SynapseDownloadFromTableParameters.Builder()
                    .withSynapseTableId(synapseTableId).withHealthCode(healthCode)
                    .withStartDate(request.getStartDate()) .withEndDate(request.getEndDate()).withTempDir(tmpDir)
                    .withSchema(schema).build();

            // kick off async task
            SynapseDownloadFromTableTask task = new SynapseDownloadFromTableTask(param);
            task.setFileHelper(fileHelper);
            task.setSynapseHelper(synapseHelper);
            Future<SynapseDownloadFromTableResult> taskFuture = auxiliaryExecutorService.submit(task);
            taskFutureList.add(taskFuture);
        }

        return taskFutureList;
    }

    /**
     * Waits on the async tasks, then gathers up all the files downloaded. This also writes a log with error messages
     * for each failed async task.
     *
     * @param tmpDir
     *         temp directory files should be downloaded to and error log should be written to
     * @param taskFutureList
     *         list of Futures for async tasks that should be waited on
     * @return list of all files downloaded, plus error log
     * @throws IOException
     *         if writing the error log fails
     */
    private List<File> waitForAsyncTasks(File tmpDir, List<Future<SynapseDownloadFromTableResult>> taskFutureList)
            throws IOException {
        // join on threads until they're all done
        List<File> allFileList = new ArrayList<>();
        List<String> errorList = new ArrayList<>();
        for (Future<SynapseDownloadFromTableResult> oneTaskFuture : taskFutureList) {
            try {
                SynapseDownloadFromTableResult taskResult = oneTaskFuture.get();

                if (taskResult.getCsvFile() != null) {
                    allFileList.add(taskResult.getCsvFile());
                }

                if (taskResult.getBulkDownloadFile() != null) {
                    allFileList.add(taskResult.getBulkDownloadFile());
                }
            } catch (ExecutionException | InterruptedException ex) {
                String errorMsg = "Error downloading CSV: " + ex.getMessage();
                LOG.error(errorMsg, ex);
                errorList.add(errorMsg);
            }
        }

        // write errors into an error log file for the user
        if (!errorList.isEmpty()) {
            String errorLog = LINE_JOINER.join(errorList);
            File errorLogFile = fileHelper.newFile(tmpDir, ERROR_LOG_FILE_NAME);

            try (Writer errorLogFileWriter = fileHelper.getWriter(errorLogFile)) {
                errorLogFileWriter.write(errorLog);
            }

            allFileList.add(errorLogFile);
        }
        return allFileList;
    }

    /**
     * Helper method that calls through to ZipHelper. This also adds timing metrics and logging.
     *
     * @param allFileList
     *         list of files to zip up
     * @param masterZipFile
     *         file to zip to
     * @throws IOException
     *         if zipping the files fails
     */
    private void zipFiles(List<File> allFileList, File masterZipFile) throws IOException {
        Stopwatch zipStopwatch = Stopwatch.createStarted();
        try {
            zipHelper.zip(allFileList, masterZipFile);
        } finally {
            zipStopwatch.stop();
            LOG.info("Zipping to file " + masterZipFile.getAbsolutePath() + " took " +
                    zipStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * Helper method that calls through to the S3Helper. This also adds timing metrics and logging.
     *
     * @param masterZipFile
     *         zip file to upload to S3
     */
    private void uploadToS3(File masterZipFile) {
        // upload to S3
        Stopwatch uploadToS3Stopwatch = Stopwatch.createStarted();
        try {
            s3Helper.writeFileToS3(userdataBucketName, masterZipFile.getName(), masterZipFile);
        } finally {
            uploadToS3Stopwatch.stop();
            LOG.info("Uploading file " + masterZipFile.getAbsolutePath() + " to S3 took " +
                    uploadToS3Stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        }
    }

    /**
     * Generate the pre-signed URL for the master zip file.
     *
     * @param masterZipFilename
     *         master zip file name
     * @return pre-signed URL info, including the actual URL and the expiration time.
     */
    private PresignedUrlInfo generatePresignedUrlInfo(String masterZipFilename) {
        // Get pre-signed URL for download. This URL expires after a number of hours, defined by configuration.
        DateTime expirationTime = now().plusHours(urlExpirationHours);
        URL presignedUrl = s3Helper.generatePresignedUrl(userdataBucketName, masterZipFilename, expirationTime,
                HttpMethod.GET);
        return new PresignedUrlInfo.Builder().withUrl(presignedUrl).withExpirationTime(expirationTime).build();
    }

    /**
     * <p>
     * Cleans up all the files we wrote for the given run. If the files are null or don't exist, skip them.
     * </p>
     * <p>
     * This is package-scoped to allow direct access from unit tests.
     * </p>
     *
     * @param allFileList
     *         list of files returned by the async tasks
     * @param masterZipFile
     *         master zip file, zip of the files returned by the async tasks
     * @param tmpDir
     *         temp dir containing all these files, obviously deleted last
     */
    void cleanupFiles(List<File> allFileList, File masterZipFile, File tmpDir) {
        // cleanup files
        List<File> filesToDelete = new ArrayList<>();
        if (allFileList != null) {
            filesToDelete.addAll(allFileList);
        }
        filesToDelete.add(masterZipFile);

        for (File oneFileToDelete : filesToDelete) {
            if (oneFileToDelete == null || !fileHelper.fileExists(oneFileToDelete)) {
                // No file. No need to cleanup.
                continue;
            }

            try {
                fileHelper.deleteFile(oneFileToDelete);
            } catch (IOException ex) {
                LOG.error("Error deleting file " + oneFileToDelete.getAbsolutePath());
            }
        }

        // clean up temp dir
        try {
            fileHelper.deleteDir(tmpDir);
        } catch (IOException ex) {
            LOG.error("Error deleting temp dir " + tmpDir.getAbsolutePath());
        }
    }

    /**
     * Helper method which returns "now". This is moved into a member method to enable mocking and is package-scoped to
     * make it available to unit tests.
     */
    DateTime now() {
        return DateTime.now();
    }
}
