package org.sagebionetworks.bridge.udd.dynamodb;

import com.google.common.base.Strings;

public class StudyInfo {
    private final String name;
    private final String studyId;
    private final String stormpathHref;
    private final String supportEmail;

    private StudyInfo(String name, String studyId, String stormpathHref, String supportEmail) {
        this.name = name;
        this.studyId = studyId;
        this.stormpathHref = stormpathHref;
        this.supportEmail = supportEmail;
    }

    public String getName() {
        return name;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getStormpathHref() {
        return stormpathHref;
    }

    public String getSupportEmail() {
        return supportEmail;
    }

    public static class Builder {
        private String name;
        private String studyId;
        private String stormpathHref;
        private String supportEmail;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        public Builder withStormpathHref(String stormpathHref) {
            this.stormpathHref = stormpathHref;
            return this;
        }

        public Builder withSupportEmail(String supportEmail) {
            this.supportEmail = supportEmail;
            return this;
        }

        public StudyInfo build() {
            if (Strings.isNullOrEmpty(name)) {
                throw new IllegalStateException("name must be specified");
            }

            if (Strings.isNullOrEmpty(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (Strings.isNullOrEmpty(stormpathHref)) {
                throw new IllegalStateException("stormpathHref must be specified");
            }

            if (Strings.isNullOrEmpty(supportEmail)) {
                throw new IllegalStateException("supportEmail must be specified");
            }

            return new StudyInfo(name, studyId, stormpathHref, supportEmail);
        }
    }
}
