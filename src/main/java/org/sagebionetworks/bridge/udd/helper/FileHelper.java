package org.sagebionetworks.bridge.udd.helper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.springframework.stereotype.Component;

/**
 * Helper to abstract away file system operations. This allows us to run mock unit tests without hitting the real file
 * system.
 */
@Component
public class FileHelper {
    /** Non-static createTempDir directory. */
    public File createTempDir() {
        return Files.createTempDir();
    }

    /**
     * Delete the specified directory. This is used so that mock file systems can keep track of files. Even though
     * this is identical to deleteFile(), having a separate deleteDir() makes mocking and testing easier.
     */
    public void deleteDir(File dir) {
        dir.delete();
    }

    /** Delete the specified file. This is used so that mock file systems can keep track of files. */
    public void deleteFile(File file) {
        file.delete();
    }

    /** Non-static get stream. */
    public OutputStream getStream(File file) throws IOException {
        return new FileOutputStream(file);
    }

    /** Non-static File constructor. */
    public File newFile(File parent, String filename) {
        return new File(parent, filename);
    }

    /** Non-static method to write bytes to the specified file. */
    public void writeBytesToFile(byte[] from, File to) throws IOException {
        Files.write(from, to);
    }

    /** Non-static method to write from the specified file to the specified stream. */
    public void writeFileToStream(File from, OutputStream to) throws IOException {
        Files.copy(from, to);
    }

    /** Non-static method to write chars to the specified file, using UTF-8 encoding. */
    public void writeStringToFile(String from, File to) throws IOException {
        Files.write(from, to, Charsets.UTF_8);
    }
}
