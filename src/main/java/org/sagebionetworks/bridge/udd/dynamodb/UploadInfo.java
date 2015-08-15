package org.sagebionetworks.bridge.udd.dynamodb;

import com.google.common.base.Strings;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

public class UploadInfo {
    private final String id;
    private final LocalDate uploadDate;

    private UploadInfo(String id, LocalDate uploadDate) {
        this.id = id;
        this.uploadDate = uploadDate;
    }

    public String getId() {
        return id;
    }

    public LocalDate getUploadDate() {
        return uploadDate;
    }

    public static class Builder {
        private String id;
        private LocalDate uploadDate;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withUploadDate(LocalDate uploadDate) {
            this.uploadDate = uploadDate;
            return this;
        }

        public Builder withUploadDate(String uploadDate) {
            this.uploadDate = LocalDate.parse(uploadDate, ISODateTimeFormat.date());
            return this;
        }

        public UploadInfo build() {
            if (Strings.isNullOrEmpty(id)) {
                throw new IllegalStateException("id must be specified");
            }

            if (uploadDate == null) {
                throw new IllegalStateException("uploadDate must be specified");
            }

            return new UploadInfo(id, uploadDate);
        }
    }
}
