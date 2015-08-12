package org.sagebionetworks.bridge.udd.helper;

import java.io.File;
import java.util.List;

import javax.annotation.Resource;

import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;

import org.sagebionetworks.bridge.udd.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;
import org.sagebionetworks.bridge.udd.dynamodb.UploadInfo;

public class S3Packager {
    // TODO: move bucket name to configs
    private static final String S3_BUCKET_NAME = "org-sagebridge-upload-dwaynejeng";

    private LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache;
    private S3Helper s3Helper;

    @Resource(name = "cmsEncryptorCache")
    public void setCmsEncryptorCache(LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    @Autowired
    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    public void packageFilesForUploadList(BridgeUddRequest request, List<UploadInfo> uploadInfoList) {
        // Download files into temp dir. Files will be named in the pattern YYYY-MM-DD-[UploadId]. This will allow
        // users to organize their data by date.
        File tmpDir = Files.createTempDir();
        for (UploadInfo oneUploadInfo : uploadInfoList) {
            String uploadId = oneUploadInfo.getId();
            LocalDate uploadDate = oneUploadInfo.getUploadDate();

            try {
                // download
                byte[] encryptedUpload = s3Helper.readS3FileAsBytes(S3_BUCKET_NAME, uploadId);

                // decrypt
                //cmsEncryptorCache.get(
            } catch (Exception ex) {
                // TODO
            }
        }
    }
}
