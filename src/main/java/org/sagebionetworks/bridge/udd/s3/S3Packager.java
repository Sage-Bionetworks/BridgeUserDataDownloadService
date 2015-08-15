package org.sagebionetworks.bridge.udd.s3;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.config.EnvironmentConfig;
import org.sagebionetworks.bridge.udd.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

@Component
public class S3Packager {
    private static final Logger LOG = LoggerFactory.getLogger(S3Packager.class);

    // TODO: move to configs
    private static final int URL_EXPIRATION_HOURS = 24;

    private static final Joiner LINE_JOINER = Joiner.on('\n');
    private static final int PROGRESS_INTERVAL = 100;

    private LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache;
    private EnvironmentConfig envConfig;
    private S3Helper s3Helper;

    @Resource(name = "cmsEncryptorCache")
    public final void setCmsEncryptorCache(LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    @Autowired
    public void setEnvConfig(EnvironmentConfig envConfig) {
        this.envConfig = envConfig;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public PresignedUrlInfo packageFilesForUploadList(BridgeUddRequest request, List<UploadInfo> uploadInfoList)
            throws IOException {
        // Download files into temp dir. Files will be named in the pattern "YYYY-MM-DD-[UploadId].zip". This will
        // allow users to organize their data by date.
        int userHash = request.getUsername().hashCode();
        int numUploads = uploadInfoList.size();
        LOG.info("Downloading " + numUploads + " files from S3 for hash[username]=" + userHash);

        File tmpDir = Files.createTempDir();
        List<File> uploadFileList = new ArrayList<>();
        List<String> errorList = new ArrayList<>();
        int numDownloaded = 0;
        for (UploadInfo oneUploadInfo : uploadInfoList) {
            String uploadId = oneUploadInfo.getId();
            LocalDate uploadDate = oneUploadInfo.getUploadDate();
            String uploadDateStr = uploadDate.toString(ISODateTimeFormat.date());

            try {
                // download and decrypt
                String uploadBucketName = envConfig.getProperty("upload.bucket");
                byte[] encryptedUploadData = s3Helper.readS3FileAsBytes(uploadBucketName, uploadId);
                BcCmsEncryptor encryptor = cmsEncryptorCache.get(request.getStudyId());
                byte[] uploadData = encryptor.decrypt(encryptedUploadData);

                // write to temp dir
                String uploadFilename = uploadDateStr + '-' + uploadId + ".zip";
                File uploadFile = new File(tmpDir, uploadFilename);
                Files.write(uploadData, uploadFile);
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
            File errorLogFile = new File(tmpDir, "error.log");
            Files.write(errorLog, errorLogFile, Charsets.UTF_8);
            uploadFileList.add(errorLogFile);
        }

        // Zip up all upload files. Filename is "userdata-[startDate]-to-[endDate]-[random guid].zip". This allows the
        // filename to be unique, user-friendly, and contain no identifying info.
        String startDateString = request.getStartDate().toString(ISODateTimeFormat.date());
        String endDateString = request.getEndDate().toString(ISODateTimeFormat.date());
        String randomGuid = UUID.randomUUID().toString();
        String masterZipFilename = "userdata-" + startDateString + "-to-" + endDateString + "-" + randomGuid + ".zip";
        File masterZipFile = new File(tmpDir, masterZipFilename);

        LOG.info("Compressing " + numUploads + " files for hash[username]=" + userHash);
        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                new FileOutputStream(masterZipFile)))) {
            int numZipped = 0;
            for (File oneUploadFile : uploadFileList) {
                ZipEntry oneZipEntry = new ZipEntry(oneUploadFile.getName());
                zipOutputStream.putNextEntry(oneZipEntry);
                Files.copy(oneUploadFile, zipOutputStream);
                zipOutputStream.closeEntry();
                oneUploadFile.delete();

                if (++numZipped % PROGRESS_INTERVAL == 0) {
                    LOG.info("Compressed " + numZipped + " of " + numUploads + " for hash[username]=" + userHash);
                }
            }
        }

        // upload to S3
        String userdataBucketName = envConfig.getProperty("userdata.bucket");
        s3Helper.getS3Client().putObject(userdataBucketName, masterZipFilename, masterZipFile);

        // cleanup
        masterZipFile.delete();
        tmpDir.delete();

        // Get pre-signed URL for download. This URL expires after a number of hours, defined by configuration.
        DateTime expirationTime = DateTime.now().plusHours(URL_EXPIRATION_HOURS);
        URL presignedUrl = s3Helper.getS3Client().generatePresignedUrl(userdataBucketName, masterZipFilename,
                expirationTime.toDate(), HttpMethod.GET);
        return new PresignedUrlInfo.Builder().withUrl(presignedUrl).withExpirationTime(expirationTime).build();
    }
}
