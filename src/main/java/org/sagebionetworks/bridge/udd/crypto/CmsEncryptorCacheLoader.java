package org.sagebionetworks.bridge.udd.crypto;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import com.google.common.cache.CacheLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.s3.S3Helper;

/**
 * This is the cache loader that supports loading CMS encryptors on demand, keyed by the study ID. If the study's
 * encryptor is already in the cache, this returns that encryptor. If it isn't, this study will pull the PEM fils for
 * the cert and private key from the configured S3 bucket and construct an encryptor using those encryption materials.
 */
// TODO: This is copy-pasted and refactored from BridgePF. Refactor this into a shared library.
@Component
public class CmsEncryptorCacheLoader extends CacheLoader<String, BcCmsEncryptor> {
    private static final String PEM_FILENAME_FORMAT = "%s.pem";

    // TODO: move these to configs
    private static final String CERT_BUCKET = "org-sagebridge-upload-cms-cert-local";
    private static final String PRIV_KEY_BUCKET = "org-sagebridge-upload-cms-priv-local";

    private S3Helper s3Helper;

    /** S3 helper, configured by Spring. */
    @Autowired
    public void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    /** {@inheritDoc} */
    @Override
    public BcCmsEncryptor load(String studyId) throws CertificateEncodingException, IOException {
        String pemFileName = String.format(PEM_FILENAME_FORMAT, studyId);

        // download certificate
        String certPem = s3Helper.readS3FileAsString(CERT_BUCKET, pemFileName);
        X509Certificate cert = PemUtils.loadCertificateFromPem(certPem);

        // download private key
        String privKeyPem = s3Helper.readS3FileAsString(PRIV_KEY_BUCKET, pemFileName);
        PrivateKey privKey = PemUtils.loadPrivateKeyFromPem(privKeyPem);

        return new BcCmsEncryptor(cert, privKey);
    }
}
