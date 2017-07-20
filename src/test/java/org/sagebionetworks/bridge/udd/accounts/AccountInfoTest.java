package org.sagebionetworks.bridge.udd.accounts;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AccountInfoTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*emailAddress.*")
    public void nullEmailAddress() {
        new AccountInfo.Builder().withHealthId("dummy-health-id").withUsername("dummy-username").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*emailAddress.*")
    public void emptyEmailAddress() {
        new AccountInfo.Builder().withEmailAddress("").withHealthId("dummy-health-id").withUsername("dummy-username")
                .build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthId.*")
    public void nullHealthId() {
        new AccountInfo.Builder().withEmailAddress("dummy-email@example.com").withUsername("dummy-username").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*healthId.*")
    public void emptyHealthId() {
        new AccountInfo.Builder().withEmailAddress("dummy-email@example.com").withHealthId("")
                .withUsername("dummy-username").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*username.*")
    public void nullUsername() {
        new AccountInfo.Builder().withEmailAddress("dummy-email@example.com").withHealthId("dummy-health-id").build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*username.*")
    public void emptyUsername() {
        new AccountInfo.Builder().withEmailAddress("dummy-email@example.com").withHealthId("dummy-health-id")
                .withUsername("").build();
    }

    @Test
    public void healthIdAndUsername() {
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthId("dummy-health-id").withUsername("dummy-username").build();
        assertEquals(accountInfo.getEmailAddress(), "dummy-email@example.com");
        assertEquals(accountInfo.getHealthId(), "dummy-health-id");
        assertEquals(accountInfo.getUsername(), "dummy-username");

        // log ID contains a hash of the username (and the prefix "hash[username]="), but not the username itself
        String logId = accountInfo.getLogId();
        assertTrue(logId.startsWith("hash[username]="));
        assertFalse(logId.contains("dummy-username"));
    }

    @Test
    public void healthCodeAndUserId() {
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthCode("dummy-health-code").withUserId("dummy-user-id").build();
        assertEquals(accountInfo.getEmailAddress(), "dummy-email@example.com");
        assertEquals(accountInfo.getHealthCode(), "dummy-health-code");
        assertEquals(accountInfo.getUserId(), "dummy-user-id");

        // log ID is just user ID
        assertEquals(accountInfo.getLogId(), accountInfo.getUserId());
    }
}
