package org.sagebionetworks.bridge.udd.accounts;

import com.google.common.base.Strings;

/** Encapsulates account information. */
public class AccountInfo {
    private final String emailAddress;
    private final String healthId;
    private final String username;

    /** Private constructor. Construction should go through the builder. */
    private AccountInfo(String emailAddress, String healthId, String username) {
        this.emailAddress = emailAddress;
        this.healthId = healthId;
        this.username = username;
    }

    /** Account's registered email address. */
    public String getEmailAddress() {
        return emailAddress;
    }

    /** Account's health ID, which is used to obtain the health code. */
    public String getHealthId() {
        return healthId;
    }

    /** Account's username. */
    public String getUsername() {
        return username;
    }

    /** Builder for AccountInfo. */
    public static class Builder {
        private String emailAddress;
        private String healthId;
        private String username;

        /** @see AccountInfo#getEmailAddress */
        public Builder withEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
            return this;
        }

        /** @see AccountInfo#getHealthId */
        public Builder withHealthId(String healthId) {
            this.healthId = healthId;
            return this;
        }

        /** @see AccountInfo#getUsername */
        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        /** Builds an AccountInfo and validates that all fields are specified. */
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
