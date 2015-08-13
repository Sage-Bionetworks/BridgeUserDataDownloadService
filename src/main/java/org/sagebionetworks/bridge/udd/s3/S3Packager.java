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
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

@Component
public class S3Packager {
    private static final Logger LOG = LoggerFactory.getLogger(S3Packager.class);

    // TODO: move bucket name and expiration period to configs
    private static final String UPLOAD_BUCKET_NAME = "org-sagebridge-upload-dwaynejeng";
    private static final String USERDATA_BUCKET_NAME = "org-sagebridge-userdata-dwaynejeng";
    private static final int URL_EXPIRATION_HOURS = 24;

    private LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache;
    private S3Helper s3Helper;

    @Resource(name = "cmsEncryptorCache")
    public final void setCmsEncryptorCache(LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public PresignedUrlInfo packageFilesForUploadList(BridgeUddRequest request, List<UploadInfo> uploadInfoList)
            throws IOException {
        // Download files into temp dir. Files will be named in the pattern "YYYY-MM-DD-[UploadId].zip". This will
        // allow users to organize their data by date.
        File tmpDir = Files.createTempDir();
        List<File> uploadFileList = new ArrayList<>();
        for (UploadInfo oneUploadInfo : uploadInfoList) {
            String uploadId = oneUploadInfo.getId();
            LocalDate uploadDate = oneUploadInfo.getUploadDate();
            String uploadDateStr = uploadDate.toString(ISODateTimeFormat.date());

            try {
                // download and decrypt
                byte[] encryptedUploadData = s3Helper.readS3FileAsBytes(UPLOAD_BUCKET_NAME, uploadId);
                BcCmsEncryptor encryptor = cmsEncryptorCache.get(request.getStudyId());
                byte[] uploadData = encryptor.decrypt(encryptedUploadData);

                // write to temp dir
                String uploadFilename = uploadDateStr + '-' + uploadId + ".zip";
                File uploadFile = new File(tmpDir, uploadFilename);
                Files.write(uploadData, uploadFile);
                uploadFileList.add(uploadFile);
            } catch (Exception ex) {
                LOG.error("Error processing upload " + uploadId + ": " + ex.getMessage(), ex);
                // TODO: write error file into the temp dir, so users know something went wrong
            }
        }

        // Zip up all upload files. Filename is "userdata-[startDate]-to-[endDate]-[random guid].zip". This allows the
        // filename to be unique, user-friendly, and contain no identifying info.
        String startDateString = request.getStartDate().toString(ISODateTimeFormat.date());
        String endDateString = request.getEndDate().toString(ISODateTimeFormat.date());
        String randomGuid = UUID.randomUUID().toString();
        String masterZipFilename = "userdata-" + startDateString + "-to-" + endDateString + "-" + randomGuid + ".zip";
        File masterZipFile = new File(tmpDir, masterZipFilename);

        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(
                new FileOutputStream(masterZipFile)))) {
            for (File oneUploadFile : uploadFileList) {
                ZipEntry oneZipEntry = new ZipEntry(oneUploadFile.getName());
                zipOutputStream.putNextEntry(oneZipEntry);
                Files.copy(oneUploadFile, zipOutputStream);
                zipOutputStream.closeEntry();

                oneUploadFile.delete();
            }
        }

        // upload to S3
        s3Helper.getS3Client().putObject(USERDATA_BUCKET_NAME, masterZipFilename, masterZipFile);

        // Get pre-signed URL for download. This URL expires after a number of hours, defined by configuration.
        DateTime expirationTime = DateTime.now().plusHours(URL_EXPIRATION_HOURS);
        URL presignedUrl = s3Helper.getS3Client().generatePresignedUrl(USERDATA_BUCKET_NAME, masterZipFilename,
                expirationTime.toDate(), HttpMethod.GET);
        return new PresignedUrlInfo.Builder().withUrl(presignedUrl).withExpirationTime(expirationTime).build();
    }
}
