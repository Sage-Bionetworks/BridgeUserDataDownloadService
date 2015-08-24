package org.sagebionetworks.bridge.udd.dynamodb;

import com.google.common.base.Strings;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

/** Encapsulates upload metadata. */
public class UploadInfo {
    private final String id;
    private final LocalDate uploadDate;

    /** Private constructor. To construct, use builder. */
    private UploadInfo(String id, LocalDate uploadDate) {
        this.id = id;
        this.uploadDate = uploadDate;
    }

    /** Upload ID. */
    public String getId() {
        return id;
    }

    /** Upload date (date that the upload was recorded in Bridge. */
    public LocalDate getUploadDate() {
        return uploadDate;
    }

    /** UploadInfo builder. */
    public static class Builder {
        private String id;
        private LocalDate uploadDate;

        /** @see UploadInfo#getId */
        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        /** @see UploadInfo#getUploadDate */
        public Builder withUploadDate(LocalDate uploadDate) {
            this.uploadDate = uploadDate;
            return this;
        }

        /**
         * Convenience method for taking upload date as a string instead of a LocalDate.
         *
         * @see UploadInfo#getUploadDate
         */
        public Builder withUploadDate(String uploadDate) {
            if (Strings.isNullOrEmpty(uploadDate)) {
                throw new IllegalArgumentException("uploadDate must be specified");
            }

            this.uploadDate = LocalDate.parse(uploadDate, ISODateTimeFormat.date());
            return this;
        }

        /** Builds an UploadInfo and validates that all fields are specified. */
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
