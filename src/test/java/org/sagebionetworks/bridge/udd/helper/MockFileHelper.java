package org.sagebionetworks.bridge.udd.helper;

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

import com.google.common.base.Charsets;

// mock file helper - store files in an in-memory map
public class MockFileHelper extends FileHelper {
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
    public OutputStream getOutputStream(File file) {
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
