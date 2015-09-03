package org.sagebionetworks.bridge.udd.accounts;

import javax.annotation.Resource;

import com.stormpath.sdk.account.Account;
import com.stormpath.sdk.account.AccountList;
import com.stormpath.sdk.account.Accounts;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.directory.Directory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.crypto.Encryptor;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;

/** Helper class to get account info from Stormpath. */
@Component
public class StormpathHelper {
    private Encryptor healthCodeEncryptor;
    private Client stormpathClient;

    /** AES GCM encryptor, used to decrypt the health ID from Stormpath custom data. */
    @Resource(name = "healthCodeEncryptor")
    public final void setHealthCodeEncryptor(Encryptor healthCodeEncryptor) {
        this.healthCodeEncryptor = healthCodeEncryptor;
    }

    /** Stormpath client. */
    @Autowired
    public final void setStormpathClient(Client stormpathClient) {
        this.stormpathClient = stormpathClient;
    }

    /**
     * Fetches account info for the given username in the given study.
     *
     * @param studyInfo
     *         study that the user lives in, must be non-null
     * @param username
     *         username of the account to fetch, must be non-null
     * @return the requested account, will be non-null
     */
    public AccountInfo getAccount(StudyInfo studyInfo, String username) {
        // get account from Stormpath
        Directory directory = stormpathClient.getResource(studyInfo.getStormpathHref(), Directory.class);
        AccountList accountList = directory.getAccounts(Accounts.where(Accounts.username().eqIgnoreCase(username))
                .withCustomData());
        Account account = accountList.single();

        // Decrypt health ID. It's called "code" in Stormpath, but it's actually the Health ID.
        String encryptedHealthId = (String) account.getCustomData().get(studyInfo.getStudyId() + "_code");
        String healthId = healthCodeEncryptor.decrypt(encryptedHealthId);

        // build account info
        return new AccountInfo.Builder().withEmailAddress(account.getEmail()).withHealthId(healthId)
                .withUsername(username).build();
    }
}
