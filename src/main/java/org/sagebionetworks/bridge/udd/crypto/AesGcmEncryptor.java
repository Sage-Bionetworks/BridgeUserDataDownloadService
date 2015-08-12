package org.sagebionetworks.bridge.udd.crypto;

import java.nio.charset.StandardCharsets;
import java.security.Security;

import org.apache.shiro.codec.Base64;
import org.apache.shiro.crypto.AesCipherService;
import org.apache.shiro.crypto.OperationMode;
import org.apache.shiro.crypto.PaddingScheme;
import org.apache.shiro.util.ByteSource;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

// TODO: This is copy-pasted from BridgePF. Refactor this into a shared library.
public class AesGcmEncryptor {
    public static final Integer VERSION = new Integer(2);

    AesGcmEncryptor() {
        aesCipher = createCipher();
        key = Base64.encodeToString(aesCipher.generateNewKey(KEY_BIT_SIZE).getEncoded());
    }

    public AesGcmEncryptor(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty.");
        }
        this.key = key;
        aesCipher = createCipher();
    }

    public Integer getVersion() {
        return VERSION;
    }

    public String encrypt(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Text to encrypt cannot be null.");
        }
        byte[] base64 = Base64.encode(text.getBytes(StandardCharsets.UTF_8));
        ByteSource bytes = aesCipher.encrypt(base64, Base64.decode(key));
        return bytes.toBase64();
    }

    public String decrypt(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Text to decrypt cannot be null.");
        }
        ByteSource bytes = aesCipher.decrypt(Base64.decode(text), Base64.decode(key));
        return Base64.decodeToString(bytes.getBytes());
    }

    private AesCipherService createCipher() {
        Security.addProvider(new BouncyCastleProvider());
        AesCipherService cipher = new AesCipherService();
        cipher.setKeySize(KEY_BIT_SIZE);
        cipher.setMode(OperationMode.GCM);
        cipher.setPaddingScheme(PaddingScheme.NONE);
        return cipher;
    }

    private static final int KEY_BIT_SIZE = 256;
    private final AesCipherService aesCipher;
    private final String key;
}
