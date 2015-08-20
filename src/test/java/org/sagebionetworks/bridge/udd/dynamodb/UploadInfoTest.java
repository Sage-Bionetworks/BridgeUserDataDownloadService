package org.sagebionetworks.bridge.udd.dynamodb;

import static org.testng.Assert.assertEquals;

import org.joda.time.LocalDate;
import org.testng.annotations.Test;

public class UploadInfoTest {
    @Test(expectedExceptions = IllegalStateException.class)
    public void nullUploadId() {
        new UploadInfo.Builder().withUploadDate("2015-08-19").build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void emptyUploadId() {
        new UploadInfo.Builder().withId("").withUploadDate("2015-08-19").build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void noUploadDate() {
        new UploadInfo.Builder().withId("test-upload").build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void nullUploadDate() {
        new UploadInfo.Builder().withId("test-upload").withUploadDate((String) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void emptyUploadDate() {
        new UploadInfo.Builder().withId("test-upload").withUploadDate("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void malformedUploadDate() {
        new UploadInfo.Builder().withId("test-upload").withUploadDate("foobarbaz");
    }

    @Test
    public void uploadDateAsString() {
        UploadInfo uploadInfo = new UploadInfo.Builder().withId("test-upload").withUploadDate("2015-08-19").build();
        assertEquals(uploadInfo.getId(), "test-upload");
        assertEquals(uploadInfo.getUploadDate().toString(), "2015-08-19");
    }

    @Test
    public void uploadDateAsObject() {
        UploadInfo uploadInfo = new UploadInfo.Builder().withId("test-upload")
                .withUploadDate(LocalDate.parse("2015-08-19")).build();
        assertEquals(uploadInfo.getId(), "test-upload");
        assertEquals(uploadInfo.getUploadDate().toString(), "2015-08-19");
    }
}
