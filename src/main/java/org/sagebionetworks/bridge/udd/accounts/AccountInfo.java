package org.sagebionetworks.bridge.udd.accounts;

import com.google.common.base.Strings;

public class AccountInfo {
    private final String emailAddress;
    private final String healthId;
    private final String username;

    private AccountInfo(String emailAddress, String healthId, String username) {
        this.emailAddress = emailAddress;
        this.healthId = healthId;
        this.username = username;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public String getHealthId() {
        return healthId;
    }

    public String getUsername() {
        return username;
    }

    public static class Builder {
        private String emailAddress;
        private String healthId;
        private String username;

        public Builder withEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
            return this;
        }

        public Builder withHealthId(String healthId) {
            this.healthId = healthId;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public AccountInfo build() {
            if (Strings.isNullOrEmpty(emailAddress)) {
                throw new IllegalStateException("emailAddress must be specified");
            }

            if (Strings.isNullOrEmpty(healthId)) {
                throw new IllegalStateException("healthId must be specified");
            }

            if (Strings.isNullOrEmpty(username)) {
                throw new IllegalStateException("username must be specified");
            }

            return new AccountInfo(emailAddress, healthId, username);
        }
    }
}
