package org.sagebionetworks.bridge.udd.accounts;

import static org.testng.Assert.assertEquals;

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
    public void happyCase() {
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthId("dummy-health-id").withUsername("dummy-username").build();
        assertEquals(accountInfo.getEmailAddress(), "dummy-email@example.com");
        assertEquals(accountInfo.getHealthId(), "dummy-health-id");
        assertEquals(accountInfo.getUsername(), "dummy-username");
    }
}
