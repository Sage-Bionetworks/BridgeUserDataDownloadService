package org.sagebionetworks.bridge.udd.worker;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;

import org.sagebionetworks.bridge.udd.helper.LocalDateToStringSerializer;

@JsonDeserialize(builder = BridgeUddRequest.Builder.class)
public class BridgeUddRequest {
    private final String studyId;
    private final String username;
    private final LocalDate startDate;
    private final LocalDate endDate;

    private BridgeUddRequest(String studyId, String username, LocalDate startDate, LocalDate endDate) {
        this.studyId = studyId;
        this.username = username;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getUsername() {
        return username;
    }

    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getStartDate() {
        return startDate;
    }

    @JsonSerialize(using = LocalDateToStringSerializer.class)
    public LocalDate getEndDate() {
        return endDate;
    }

    public static class Builder {
        private String studyId;
        private String username;
        private LocalDate startDate;
        private LocalDate endDate;

        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        @JsonDeserialize(using = LocalDateDeserializer.class)
        public Builder withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }

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
