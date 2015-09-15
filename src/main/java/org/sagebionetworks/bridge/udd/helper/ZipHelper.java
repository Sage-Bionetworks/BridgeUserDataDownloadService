package org.sagebionetworks.bridge.udd.helper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.common.base.Charsets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** This helper zips the given input files into the given target file. */
@Component
public class ZipHelper {
    private FileHelper fileHelper;

    /** File helper, used to read data from the input files and write to the output file. */
    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /**
     * Zips the list of input files and writes the result to the output file.
     *
     * @param fromList
     *         list of input files
     * @param to
     *         output file to write the zip file to
     * @throws IOException
     *         if reading from input or writing to output fails
     */
    public void zip(List<File> fromList, File to) throws IOException {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileHelper.getOutputStream(to));
                ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream, Charsets.UTF_8)) {
            for (File oneFromFile : fromList) {
                ZipEntry oneZipEntry = new ZipEntry(oneFromFile.getName());
                zipOutputStream.putNextEntry(oneZipEntry);
                fileHelper.writeFileToStream(oneFromFile, zipOutputStream);
                zipOutputStream.closeEntry();
            }
        }
    }
}
