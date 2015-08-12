package org.sagebionetworks.bridge.udd.dynamodb;

import com.google.common.base.Strings;

public class StudyInfo {
    private final String studyId;
    private final String stormpathHref;

    private StudyInfo(String studyId, String stormpathHref) {
        this.studyId = studyId;
        this.stormpathHref = stormpathHref;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getStormpathHref() {
        return stormpathHref;
    }

    public static class Builder {
        private String studyId;
        private String stormpathHref;

        public Builder withStudyId(String studyId) {
            this.studyId = studyId;
            return this;
        }

        public Builder withStormpathHref(String stormpathHref) {
            this.stormpathHref = stormpathHref;
            return this;
        }

        public StudyInfo build() {
            if (Strings.isNullOrEmpty(studyId)) {
                throw new IllegalStateException("studyId must be specified");
            }

            if (Strings.isNullOrEmpty(stormpathHref)) {
                throw new IllegalStateException("stormpathHref must be specified");
            }

            return new StudyInfo(studyId, stormpathHref);
        }
    }
}
