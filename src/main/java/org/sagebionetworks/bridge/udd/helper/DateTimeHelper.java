package org.sagebionetworks.bridge.udd.helper;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

/** Date/time helper class to allow us to mock out things like "now", for use in unit tests. */
@Component
public class DateTimeHelper {
    /** A non-static "now" method, to allow mocking in unit tests to get deterministic tests. */
    public DateTime now() {
        return DateTime.now();
    }
}
