package org.sagebionetworks.bridge.udd.synapse;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.helper.MockFileHelper;

// Deep tests for SynapsePackager.cleanupFiles()
public class SynapsePackagerCleanupTest {
    private static final byte[] EMPTY_FILE_CONTENT = new byte[0];

    private MockFileHelper mockFileHelper;
    private SynapsePackager packager;
    private File tmpDir;

    @BeforeMethod
    public void setup() {
        packager = new SynapsePackager();

        mockFileHelper = new MockFileHelper();
        packager.setFileHelper(mockFileHelper);

        tmpDir = mockFileHelper.createTempDir();
    }

    @Test
    public void nullFileList() {
        packager.cleanupFiles(null, null, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void emptyFileList() {
        packager.cleanupFiles(ImmutableList.of(), null, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void fileListWithNoMasterZip() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        packager.cleanupFiles(fileList, null, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void masterZipDoesntExist() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        File masterZipFile = mockFileHelper.newFile(tmpDir, "master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    @Test
    public void fileListAndMasterZip() throws Exception {
        List<File> fileList = ImmutableList.of(createEmptyFile("foo"), createEmptyFile("bar"), createEmptyFile("baz"));
        File masterZipFile = createEmptyFile("master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    // branch coverage
    @Test
    public void someFilesDontExist() throws Exception {
        List<File> fileList = ImmutableList.of(mockFileHelper.newFile(tmpDir, "foo"),
                mockFileHelper.newFile(tmpDir, "baz"), mockFileHelper.newFile(tmpDir, "baz"));
        File masterZipFile = mockFileHelper.newFile(tmpDir, "master.zip");
        packager.cleanupFiles(fileList, masterZipFile, tmpDir);
        assertTrue(mockFileHelper.isEmpty());
    }

    // Creates a trivial empty file, so we can test cleanup.
    private File createEmptyFile(String filename) throws Exception {
        File file = mockFileHelper.newFile(tmpDir, filename);
        touchFile(file);
        return file;
    }

    // Write an empty string to the file to ensure that it exists in our (mock) file system.
    private void touchFile(File file) throws Exception {
        try (OutputStream fileOutputStream = mockFileHelper.getOutputStream(file)) {
            fileOutputStream.write(EMPTY_FILE_CONTENT);
        }
    }
}
