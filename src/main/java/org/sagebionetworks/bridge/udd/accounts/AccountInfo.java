package org.sagebionetworks.bridge.udd.accounts;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

/** Encapsulates account information. */
public class AccountInfo {
    private final String emailAddress;
    private final String healthCode;
    private final String healthId;
    private final String userId;
    private final String username;

    /** Private constructor. Construction should go through the builder. */
    private AccountInfo(String emailAddress, String healthCode, String healthId, String userId, String username) {
        this.emailAddress = emailAddress;
        this.healthCode = healthCode;
        this.healthId = healthId;
        this.userId = userId;
        this.username = username;
    }

    /** Account's registered email address. */
    public String getEmailAddress() {
        return emailAddress;
    }

    /** Account's health code. This is available in the getParticipant API in Bridge, but not in Stormpath. */
    public String getHealthCode() {
        return healthCode;
    }

    /** Account's health ID, which is used to obtain the health code. */
    public String getHealthId() {
        return healthId;
    }

    /** Account's ID. */
    public String getUserId() {
        return userId;
    }

    /** Account's username. */
    public String getUsername() {
        return username;
    }

    /**
     * Helper method to either return userId or a hash of the username (email), for use in the logs. This can be
     * removed when we remove all the Stormpath stuff, and everything uses userId.
     */
    public String getLogId() {
        if (StringUtils.isNotBlank(userId)) {
            return userId;
        } else {
            return "hash[username]=" + username.hashCode();
        }
    }

    /** Builder for AccountInfo. */
    public static class Builder {
        private String emailAddress;
        private String healthCode;
        private String healthId;
        private String userId;
        private String username;

        /** @see AccountInfo#getEmailAddress */
        public Builder withEmailAddress(String emailAddress) {
            this.emailAddress = emailAddress;
            return this;
        }

        /** @see AccountInfo#getHealthCode */
        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        /** @see AccountInfo#getHealthId */
        public Builder withHealthId(String healthId) {
            this.healthId = healthId;
            return this;
        }

        /** @see AccountInfo#getUserId */
        public Builder withUserId(String userId) {
            this.userId = userId;
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

            if (Strings.isNullOrEmpty(healthCode) && Strings.isNullOrEmpty(healthId)) {
                throw new IllegalStateException("either healthCode or healthId must be specified");
            }

            if (Strings.isNullOrEmpty(userId) && Strings.isNullOrEmpty(username)) {
                throw new IllegalStateException("either userId or username must be specified");
            }

            return new AccountInfo(emailAddress, healthCode, healthId, userId, username);
        }
    }
}
