package org.sagebionetworks.bridge.udd.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

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
    public void deleteDir(File dir) throws IOException {
        boolean success = dir.delete();
        if (!success) {
            throw new IOException("Failed to delete directory: " + dir.getName());
        }
    }

    /** Delete the specified file. This is used so that mock file systems can keep track of files. */
    public void deleteFile(File file) throws IOException {
        boolean success = file.delete();
        if (!success) {
            throw new IOException("Failed to delete file: " + file.getName());
        }
    }

    /** Tests if the file exists. Unit tests override this to hook into our mock file system. */
    public boolean exists(File file) {
        return file.exists();
    }

    /** Non-static get output (write) stream. */
    public OutputStream getOutputStream(File file) throws FileNotFoundException {
        return new FileOutputStream(file);
    }

    /** Non-static get reader. */
    public Reader getReader(File file) throws FileNotFoundException {
        return Files.newReader(file, Charsets.UTF_8);
    }

    /** Non-static get writer. */
    public Writer getWriter(File file) throws FileNotFoundException {
        return Files.newWriter(file, Charsets.UTF_8);
    }

    /** Non-static move. */
    public void move(File from, File to) throws IOException {
        Files.move(from, to);
    }

    /** Non-static File constructor. */
    public File newFile(File parent, String filename) {
        return new File(parent, filename);
    }

    /** Non-static read lines from file. */
    public List<String> readLines(File from) throws IOException {
        return Files.readLines(from, Charsets.UTF_8);
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
