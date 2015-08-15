package org.sagebionetworks.bridge.udd.s3;

import java.net.URL;

import org.joda.time.DateTime;

public class PresignedUrlInfo {
    private final URL url;
    private final DateTime expirationTime;

    private PresignedUrlInfo(URL url, DateTime expirationTime) {
        this.url = url;
        this.expirationTime = expirationTime;
    }

    public URL getUrl() {
        return url;
    }

    public DateTime getExpirationTime() {
        return expirationTime;
    }

    public static class Builder {
        private URL url;
        private DateTime expirationTime;

        public Builder withUrl(URL url) {
            this.url = url;
            return this;
        }

        public Builder withExpirationTime(DateTime expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        public PresignedUrlInfo build() {
            if (url == null) {
                throw new IllegalStateException("url must be specified");
            }

            if (expirationTime == null) {
                throw new IllegalStateException("expiration time must be specified");
            }

            return new PresignedUrlInfo(url, expirationTime);
        }
    }
}
