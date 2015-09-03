package org.sagebionetworks.bridge.udd.accounts;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.stormpath.sdk.account.Account;
import com.stormpath.sdk.account.AccountCriteria;
import com.stormpath.sdk.account.AccountList;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.directory.CustomData;
import com.stormpath.sdk.directory.Directory;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.crypto.Encryptor;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;

public class StormpathHelperTest {
    @Test
    public void test() {
        // mock stormpath client and attributes
        CustomData mockCustomData = mock(CustomData.class);
        when(mockCustomData.get("test-study_code")).thenReturn("dummy-encrypted-health-id");

        Account mockAccount = mock(Account.class);
        when(mockAccount.getEmail()).thenReturn("dummy-email@example.com");
        when(mockAccount.getCustomData()).thenReturn(mockCustomData);

        AccountList mockAccountList = mock(AccountList.class);
        when(mockAccountList.single()).thenReturn(mockAccount);

        Directory mockDirectory = mock(Directory.class);
        when(mockDirectory.getAccounts(any(AccountCriteria.class))).thenReturn(mockAccountList);

        Client mockStormpathClient = mock(Client.class);
        when(mockStormpathClient.getResource("dummy-stormpath-href", Directory.class)).thenReturn(mockDirectory);

        // mock health code encryptor
        Encryptor mockEncryptor = mock(Encryptor.class);
        when(mockEncryptor.decrypt("dummy-encrypted-health-id")).thenReturn("dummy-health-id");

        // create stormpath helper
        StormpathHelper stormpathHelper = new StormpathHelper();
        stormpathHelper.setHealthCodeEncryptor(mockEncryptor);
        stormpathHelper.setStormpathClient(mockStormpathClient);

        // create test study info
        StudyInfo testStudy = new StudyInfo.Builder().withStudyId("test-study").withName("Test Study")
                .withStormpathHref("dummy-stormpath-href").withSupportEmail("support@sagebase.org").build();

        // execute and validate
        AccountInfo accountInfo = stormpathHelper.getAccount(testStudy, "dummy-username");
        assertEquals(accountInfo.getEmailAddress(), "dummy-email@example.com");
        assertEquals(accountInfo.getHealthId(), "dummy-health-id");
        assertEquals(accountInfo.getUsername(), "dummy-username");
    }
}
