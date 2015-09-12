package org.sagebionetworks.bridge.udd.s3;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Resource;

import com.amazonaws.HttpMethod;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.cache.LoadingCache;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.helper.FileHelper;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

/**
 * This class downloads the files from S3, decrypts them, packages them up into a master zip file, uploads it to S3,
 * and generates and returns a pre-signed URL for that package.
 */
@Component
public class S3Packager {
    private static final Logger LOG = LoggerFactory.getLogger(S3Packager.class);

    // TODO: move to configs
    // package-scoped to be available in tests
    static final int URL_EXPIRATION_HOURS = 24;

    // package-scoped to be available in tests
    static final String CONFIG_KEY_UPLOAD_BUCKET = "upload.bucket";
    static final String CONFIG_KEY_USERDATA_BUCKET = "userdata.bucket";

    private static final Joiner LINE_JOINER = Joiner.on('\n');
    private static final int PROGRESS_INTERVAL = 100;

    private LoadingCache<String, CmsEncryptor> cmsEncryptorCache;
    private Config envConfig;
    private FileHelper fileHelper;
    private S3Helper s3Helper;

    /**
     * Encryptor cache. This creates (and cachces) the encryptor on-demand, keyed off study. This is used to decrypt
     * files downloaded from S3.
     */
    @Resource(name = "cmsEncryptorCache")
    public final void setCmsEncryptorCache(LoadingCache<String, CmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    /** Environment configs. Used to get the upload and user data S3 buckets. */
    @Autowired
    public final void setEnvConfig(Config envConfig) {
        this.envConfig = envConfig;
    }

    /**
     * Wrapper class around the file system. Used by unit tests to test the functionality without hitting the real file
     * system.
     */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /** S3 helper. Used to download files from S3, upload files to S3, and generate pre-signed URLs. */
    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /**
     * Given the Bridge-UDD request and the corresponding list of upload infos, generate the user data package. This
     * entails downloading the files from S3, decrypting them, zipping them into a master zip file, uploading the
     * master zip file to S3, and generating and returning a pre-signed URL.
     *
     * @param request
     *         the Bridge User Data Download Service request
     * @param uploadInfoList
     *         list of upload infos, provided by the DynamoHelper
     * @return pre-signed URL info, including the URL itself and its expiration date
     * @throws IOException
     *         if reading from or writing to the file system fails
     */
    public PresignedUrlInfo packageFilesForUploadList(BridgeUddRequest request, List<UploadInfo> uploadInfoList)
            throws IOException {
        // Download files into temp dir. Files will be named in the pattern "YYYY-MM-DD-[UploadId].zip". This will
        // allow users to organize their data by date.
        int userHash = request.getUsername().hashCode();
        int numUploads = uploadInfoList.size();
        LOG.info("Downloading " + numUploads + " files from S3 for hash[username]=" + userHash);

        File tmpDir = fileHelper.createTempDir();
        List<File> uploadFileList = new ArrayList<>();
        List<String> errorList = new ArrayList<>();
        int numDownloaded = 0;
        for (UploadInfo oneUploadInfo : uploadInfoList) {
            String uploadId = oneUploadInfo.getId();
            LocalDate uploadDate = oneUploadInfo.getUploadDate();
            String uploadDateStr = uploadDate.toString();

            try {
                // download and decrypt
                String uploadBucketName = envConfig.get(CONFIG_KEY_UPLOAD_BUCKET);
                byte[] encryptedUploadData = s3Helper.readS3FileAsBytes(uploadBucketName, uploadId);
                CmsEncryptor encryptor = cmsEncryptorCache.get(request.getStudyId());
                byte[] uploadData = encryptor.decrypt(encryptedUploadData);

                // write to temp dir
                String uploadFilename = uploadDateStr + '-' + uploadId + ".zip";
                File uploadFile = fileHelper.newFile(tmpDir, uploadFilename);
                fileHelper.writeBytesToFile(uploadData, uploadFile);
                uploadFileList.add(uploadFile);
            } catch (Exception ex) {
                String errorMsg = "Error processing upload " + uploadId + ": " + ex.getMessage();
                LOG.error(errorMsg, ex);
                errorList.add(errorMsg);
            }

            if (++numDownloaded % PROGRESS_INTERVAL == 0) {
                LOG.info("Downloaded " + numDownloaded + " of " + numUploads + " for hash[username]=" + userHash);
            }
        }

        // write errors into an error log file for the user
        if (!errorList.isEmpty()) {
            String errorLog = LINE_JOINER.join(errorList);
            File errorLogFile = fileHelper.newFile(tmpDir, "error.log");
            fileHelper.writeStringToFile(errorLog, errorLogFile);
            uploadFileList.add(errorLogFile);
        }

        // Zip up all upload files. Filename is "userdata-[startDate]-to-[endDate]-[random guid].zip". This allows the
        // filename to be unique, user-friendly, and contain no identifying info.
        String startDateString = request.getStartDate().toString();
        String endDateString = request.getEndDate().toString();
        String randomGuid = UUID.randomUUID().toString();
        String masterZipFilename = "userdata-" + startDateString + "-to-" + endDateString + "-" + randomGuid + ".zip";
        File masterZipFile = fileHelper.newFile(tmpDir, masterZipFilename);

        LOG.info("Compressing " + numUploads + " files for hash[username]=" + userHash);
        try (BufferedOutputStream masterZipBufferedOutputStream =
                new BufferedOutputStream(fileHelper.getOutputStream(masterZipFile));
                ZipOutputStream zipOutputStream = new ZipOutputStream(masterZipBufferedOutputStream, Charsets.UTF_8)) {
            int numZipped = 0;
            for (File oneUploadFile : uploadFileList) {
                ZipEntry oneZipEntry = new ZipEntry(oneUploadFile.getName());
                zipOutputStream.putNextEntry(oneZipEntry);
                fileHelper.writeFileToStream(oneUploadFile, zipOutputStream);
                zipOutputStream.closeEntry();
                fileHelper.deleteFile(oneUploadFile);

                if (++numZipped % PROGRESS_INTERVAL == 0) {
                    LOG.info("Compressed " + numZipped + " of " + numUploads + " for hash[username]=" + userHash);
                }
            }
        }

        // upload to S3
        String userdataBucketName = envConfig.get(CONFIG_KEY_USERDATA_BUCKET);
        s3Helper.getS3Client().putObject(userdataBucketName, masterZipFilename, masterZipFile);

        // cleanup
        fileHelper.deleteFile(masterZipFile);
        fileHelper.deleteDir(tmpDir);

        // Get pre-signed URL for download. This URL expires after a number of hours, defined by configuration.
        DateTime expirationTime = now().plusHours(URL_EXPIRATION_HOURS);
        URL presignedUrl = s3Helper.getS3Client().generatePresignedUrl(userdataBucketName, masterZipFilename,
                expirationTime.toDate(), HttpMethod.GET);
        return new PresignedUrlInfo.Builder().withUrl(presignedUrl).withExpirationTime(expirationTime).build();
    }

    // Helper method which returns "now". This is moved into a member method to enable mocking and is package-scoped to
    // make it available to unit tests.
    DateTime now() {
        return DateTime.now();
    }
}
