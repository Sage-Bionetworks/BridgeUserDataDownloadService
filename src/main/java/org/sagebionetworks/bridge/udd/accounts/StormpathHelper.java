package org.sagebionetworks.bridge.udd.accounts;

import java.util.Iterator;
import javax.annotation.Resource;

import com.stormpath.sdk.account.Account;
import com.stormpath.sdk.account.AccountList;
import com.stormpath.sdk.account.Accounts;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.directory.Directory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.crypto.AesGcmEncryptor;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;

@Component
public class StormpathHelper {
    private AesGcmEncryptor healthCodeEncryptor;
    private Client stormpathClient;

    @Resource(name = "healthCodeEncryptor")
    public final void setHealthCodeEncryptor(AesGcmEncryptor healthCodeEncryptor) {
        this.healthCodeEncryptor = healthCodeEncryptor;
    }

    @Autowired
    public final void setStormpathClient(Client stormpathClient) {
        this.stormpathClient = stormpathClient;
    }

    public AccountInfo getAccount(StudyInfo studyInfo, String username) {
        // get account from Stormpath
        Directory directory = stormpathClient.getResource(studyInfo.getStormpathHref(), Directory.class);
        AccountList accountList = directory.getAccounts(Accounts.where(Accounts.username().eqIgnoreCase(username))
                .withCustomData());
        Iterator<Account> accountIter = accountList.iterator();
        if (!accountIter.hasNext()) {
            throw new IllegalArgumentException("account not found");
        }
        Account account = accountIter.next();

        // Decrypt health ID. It's called "code" in Stormpath, but it's actually the Health ID.
        String encryptedHealthId = (String) account.getCustomData().get(studyInfo.getStudyId() + "_code");
        String healthId = healthCodeEncryptor.decrypt(encryptedHealthId);

        // build account info
        return new AccountInfo.Builder().withEmailAddress(account.getEmail()).withHealthId(healthId)
                .withUsername(username).build();
    }
}
