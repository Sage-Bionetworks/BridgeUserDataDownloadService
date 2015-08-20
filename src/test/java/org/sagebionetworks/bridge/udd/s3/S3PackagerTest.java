package org.sagebionetworks.bridge.udd.s3;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.common.base.Charsets;
import com.google.common.cache.LoadingCache;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.config.EnvironmentConfig;
import org.sagebionetworks.bridge.udd.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.udd.helper.DateTimeHelper;
import org.sagebionetworks.bridge.udd.helper.FileHelper;

@SuppressWarnings("unchecked")
public class S3PackagerTest {
    // mock file helper - store files in an in-memory map
    private static class MockFileHelper extends FileHelper {
        private final Set<String> dirSet = new HashSet<>();
        private final Map<String, byte[]> fileMap = new HashMap<>();

        @Override
        public File createTempDir() {
            // We just need a unique file name, so just use a guid.
            String tmpDirName = UUID.randomUUID().toString();
            dirSet.add(tmpDirName);

            // For the purposes of our test, the tmp dir is at the "root" of our mock file system, so the absolute path
            // and the name are both the dir name.
            return makeMockFile(tmpDirName, tmpDirName);
        }

        @Override
        public void deleteDir(File dir) {
            String dirPath = dir.getAbsolutePath();
            if (!dirSet.contains(dirPath)) {
                throw new IllegalArgumentException("Can't deleted dir " + dirPath + ": dir doesn't exist");
            }
            dirSet.remove(dirPath);
        }

        @Override
        public void deleteFile(File file) {
            String filePath = file.getAbsolutePath();
            if (!fileMap.containsKey(filePath)) {
                throw new IllegalArgumentException("Can't delete file " + filePath + ": file doesn't exist");
            }
            fileMap.remove(filePath);
        }

        @Override
        public OutputStream getStream(File file) {
            // No need to check if the file exists, because like the real file system, the file won't be created until
            // you write.
            String filePath = file.getAbsolutePath();
            return new FileHelperOutputStream(filePath);
        }

        @Override
        public File newFile(File parent, String filename) {
            String parentDirPath = parent.getAbsolutePath();
            if (!dirSet.contains(parentDirPath)) {
                throw new IllegalArgumentException("Can't create file in dir " + parentDirPath +
                        ": dir doesn't exist");
            }

            // Don't create the file in fileMap just yet. Like in the real file system, the file doesn't get created
            // until you write.
            return makeMockFile(parentDirPath + "/" + filename, filename);
        }

        @Override
        public void writeBytesToFile(byte[] from, File to) {
            // No need to check if the file exists, because like the real file system, the file won't be created until
            // you write.
            String toPath = to.getAbsolutePath();
            fileMap.put(toPath, from);
        }

        @Override
        public void writeFileToStream(File from, OutputStream to) throws IOException {
            String fromPath = from.getAbsolutePath();
            if (!fileMap.containsKey(fromPath)) {
                throw new IllegalArgumentException("Can't write file " + fromPath + " to stream: file doesn't exist");
            }

            byte[] fromBytes = fileMap.get(fromPath);
            to.write(fromBytes);
        }

        @Override
        public void writeStringToFile(String from, File to) throws IOException {
            // No need to check if the file exists, because like the real file system, the file won't be created until
            // you write.
            String toPath = to.getAbsolutePath();
            fileMap.put(toPath, from.getBytes(Charsets.UTF_8));
        }

        // helper method to support tests
        public byte[] getBytes(File file) {
            String filePath = file.getAbsolutePath();
            if (!fileMap.containsKey(filePath)) {
                throw new IllegalArgumentException("Can't get bytes for file " + filePath + ": file doesn't exist");
            }

            return fileMap.get(filePath);
        }

        // helper method to support tests
        public boolean isEmpty() {
            return dirSet.isEmpty() && fileMap.isEmpty();
        }

        private static File makeMockFile(String absolutePath, String name) {
            File mockFile = mock(File.class);
            when(mockFile.getAbsolutePath()).thenReturn(absolutePath);
            when(mockFile.getName()).thenReturn(name);
            return mockFile;
        }

        // Hook into a ByteArrayOutputStream, but on close, write the file back into the File Helper mock file system.
        private class FileHelperOutputStream extends ByteArrayOutputStream {
            private final String filePath;

            FileHelperOutputStream(String filePath) {
                this.filePath = filePath;
            }

            @Override
            public void close() throws IOException {
                super.close();

                byte[] fileBytes = toByteArray();
                fileMap.put(filePath, fileBytes);
            }
        }
    }

    @Test
    public void test() throws Exception {
        // mock encryptor - For test mocking, this will just replace all instances of "encrypted" to "decrypted"
        BcCmsEncryptor mockEncryptor = mock(BcCmsEncryptor.class);
        when(mockEncryptor.decrypt(any(byte[].class))).thenAnswer(invocation -> {
            byte[] encryptedBytes = invocation.getArgumentAt(0, byte[].class);
            String encryptedString = new String(encryptedBytes, Charsets.UTF_8);
            String decryptedString = encryptedString.replaceAll("encrypted", "decrypted");
            return decryptedString.getBytes(Charsets.UTF_8);
        });

        LoadingCache<String, BcCmsEncryptor> mockEncryptorCache = mock(LoadingCache.class);
        when(mockEncryptorCache.get("test-study")).thenReturn(mockEncryptor);

        // mock date time helper - Instead of returning the real "now", return this fake "now"
        DateTime mockNow = DateTime.parse("2015-08-19T14:00:00-07:00");
        DateTimeHelper mockDateTimeHelper = mock(DateTimeHelper.class);
        when(mockDateTimeHelper.now()).thenReturn(mockNow);

        // mock env config - all we need is upload bucket and user data bucket
        EnvironmentConfig mockEnvConfig = mock(EnvironmentConfig.class);
        when(mockEnvConfig.getProperty(S3Packager.CONFIG_KEY_UPLOAD_BUCKET)).thenReturn("dummy-upload-bucket");
        when(mockEnvConfig.getProperty(S3Packager.CONFIG_KEY_USERDATA_BUCKET)).thenReturn("dummy-userdata-bucket");

        // mock file helper
        MockFileHelper mockFileHelper = new MockFileHelper();

        // mock S3 client
        AmazonS3Client mockS3Client = mock(AmazonS3Client.class);
        byte[][] masterZipBytesHolder = new byte[1][];
        when(mockS3Client.putObject(eq("dummy-userdata-bucket"), startsWith("userdata-2015-08-15-to-2015-08-19-"),
                any(File.class))).thenAnswer(invocation -> {
            File uploadedFile = invocation.getArgumentAt(2, File.class);
            masterZipBytesHolder[0] = mockFileHelper.getBytes(uploadedFile);
            return mock(PutObjectResult.class);
        });

        // TODO keep mocking S3 client and helper
    }
}
