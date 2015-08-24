package org.sagebionetworks.bridge.udd.worker;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.udd.helper.LocalDateToStringSerializer;

/** Represents a request to the Bridge User Data Download Service. */
@JsonDeserialize(builder = BridgeUddRequest.Builder.class)
public class BridgeUddRequest {
    private final String studyId;
    private final String username;
    private final LocalDate startDate;
    private final LocalDate endDate;

    /** Private constructor. To construct, use builder. */
    private BridgeUddRequest(String studyId, String username, LocalDate startDate, LocalDate endDate) {
        this.studyId = studyId;
        this.username = username;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /** ID of the study to get user data from. */
    public String getStudyId() {
        return studyId;
    }

    /** Username of the user requesting their data. */
    public String getUsername() {
        return username;
    }

    /** Start date (inclusive) of data to fetch. */
    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getStartDate() {
        return startDate;
    }

    /** End date (inclusive) of data to fetch. */
    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getEndDate() {
        return endDate;
    }

    /** Bridge-UDD request builder. */
    public static class Builder {
        private String studyId;
        private String username;
        private LocalDate startDate;
        private LocalDate endDate;

        /** @see BridgeUddRequest#getStudyId */
        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        /** @see BridgeUddRequest#getUsername */
        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        /** @see BridgeUddRequest#getStartDate */
        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        /** @see BridgeUddRequest#getEndDate */
        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

        /**
         * Builds a BridgeUddRequest and validates that all fields are specified and that start date isn't after end
         * date.
         */
        public BridgeUddRequest build() {
            if (Strings.isNullOrEmpty(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (Strings.isNullOrEmpty(username)) {
                throw new IllegalStateException("username must be specified");
            }

            if (startDate == null) {
                throw new IllegalStateException("startDate must be specified");
            }

            if (endDate == null) {
                throw new IllegalStateException("endDate must be specified");
            }

            if (startDate.isAfter(endDate)) {
                throw new IllegalStateException("startDate can't be after endDate");
            }

            return new BridgeUddRequest(studyId, username, startDate, endDate);
        }
    }
}
