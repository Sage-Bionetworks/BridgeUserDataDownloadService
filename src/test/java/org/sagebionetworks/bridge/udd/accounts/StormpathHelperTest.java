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
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;

public class StormpathHelperTest {
    private static final String EMAIL = "dummy-email@example.com";
    private static final String ENCRYPTED_HEALTH_ID = "dummy-encrypted-health-id";
    private static final String HEALTH_ID = "dummy-health-id";
    private static final String STORMPATH_HREF = "dummy-stormpath-href";
    private static final String USERNAME = "dummy-username";

    private static final String STUDY_ID = "test-study";
    private static final String HEALTH_CODE_KEY = STUDY_ID + "_code";
    private static final StudyInfo STUDY = new StudyInfo.Builder().withStudyId(STUDY_ID).withName("Test Study")
            .withStormpathHref(STORMPATH_HREF).withSupportEmail("support@sagebase.org").build();

    @Test
    public void test() throws Exception {
        // mock stormpath client and attributes
        Account mockAccount = makeAccount();

        AccountList mockAccountList = mock(AccountList.class);
        when(mockAccountList.getSize()).thenReturn(1);
        when(mockAccountList.single()).thenReturn(mockAccount);

        Client mockStormpathClient = mockStormpathClientWithAccountList(mockAccountList);

        // mock health code encryptor
        Encryptor mockEncryptor = mock(Encryptor.class);
        when(mockEncryptor.decrypt(ENCRYPTED_HEALTH_ID)).thenReturn(HEALTH_ID);

        // create stormpath helper
        StormpathHelper stormpathHelper = new StormpathHelper();
        stormpathHelper.setHealthCodeEncryptor(mockEncryptor);
        stormpathHelper.setStormpathClient(mockStormpathClient);

        // execute and validate
        AccountInfo accountInfo = stormpathHelper.getAccount(STUDY, USERNAME);
        assertEquals(accountInfo.getEmailAddress(), EMAIL);
        assertEquals(accountInfo.getHealthId(), HEALTH_ID);
        assertEquals(accountInfo.getUsername(), USERNAME);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "No accounts found.*")
    public void accountNotFound() throws Exception {
        testGetAccountError(0);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class, expectedExceptionsMessageRegExp =
            "Multiple accounts found.*")
    public void multipleAccounts() throws Exception {
        testGetAccountError(2);
    }

    private static void testGetAccountError(int numAccounts) throws Exception {
        // mock stormpath client
        AccountList mockAccountList = mock(AccountList.class);
        when(mockAccountList.getSize()).thenReturn(numAccounts);

        Client mockStormpathClient = mockStormpathClientWithAccountList(mockAccountList);

        // create stormpath helper
        StormpathHelper stormpathHelper = new StormpathHelper();
        stormpathHelper.setStormpathClient(mockStormpathClient);

        // execute and validate
        stormpathHelper.getAccount(STUDY, USERNAME);
    }

    private static Account makeAccount() {
        CustomData mockCustomData = mock(CustomData.class);
        when(mockCustomData.get(HEALTH_CODE_KEY)).thenReturn(ENCRYPTED_HEALTH_ID);

        Account mockAccount = mock(Account.class);
        when(mockAccount.getEmail()).thenReturn(EMAIL);
        when(mockAccount.getCustomData()).thenReturn(mockCustomData);
        return mockAccount;
    }

    private static Client mockStormpathClientWithAccountList(AccountList accountList) {
        Directory mockDirectory = mock(Directory.class);
        when(mockDirectory.getAccounts(any(AccountCriteria.class))).thenReturn(accountList);

        Client mockStormpathClient = mock(Client.class);
        when(mockStormpathClient.getResource(STORMPATH_HREF, Directory.class)).thenReturn(mockDirectory);
        return mockStormpathClient;
    }
}
