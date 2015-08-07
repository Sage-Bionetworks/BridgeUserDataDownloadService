package org.sagebionetworks.bridge.udd;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateDeserializer;
import com.google.common.base.Strings;
import org.joda.time.LocalDate;

@JsonDeserialize(builder = BridgeUddRequest.Builder.class)
public class BridgeUddRequest {
    private final String healthCode;
    private final String emailAddress;
    private final LocalDate startDate;
    private final LocalDate endDate;

    private BridgeUddRequest(String healthCode, String emailAddress, LocalDate startDate, LocalDate endDate) {
        this.healthCode = healthCode;
        this.emailAddress = emailAddress;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getHealthCode() {
        return healthCode;
    }

    public String getEmailAddress() {
        return emailAddress;
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
        private String healthCode;
        private String emailAddress;
        private LocalDate startDate;
        private LocalDate endDate;

        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        public Builder withEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
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
            if (Strings.isNullOrEmpty(healthCode)) {
                throw new IllegalStateException("healthCode must be specified");
            }

            if (Strings.isNullOrEmpty(emailAddress)) {
                throw new IllegalStateException("emailAddress must be specified");
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

            return new BridgeUddRequest(healthCode, emailAddress, startDate, endDate);
        }
    }
}
