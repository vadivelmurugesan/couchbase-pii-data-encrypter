package com.example.crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Minimal AES-256-GCM encryptor with deterministic envelope output.
 */
public final class Encryptor {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String ALGORITHM_ID = "AES-256-GCM";
    private static final int IV_LENGTH_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKey secretKey;
    private final String keyId;
    private final SecureRandom secureRandom;

    public Encryptor(SecretKey secretKey, String keyId) {
        this(secretKey, keyId, new SecureRandom());
    }

    public Encryptor(SecretKey secretKey, String keyId, SecureRandom secureRandom) {
        this.secretKey = Objects.requireNonNull(secretKey, "secretKey");
        this.keyId = Objects.requireNonNull(keyId, "keyId");
        this.secureRandom = Objects.requireNonNull(secureRandom, "secureRandom");
    }

    public EncryptionEnvelope encrypt(byte[] plaintext, String documentId) {
        Objects.requireNonNull(plaintext, "plaintext");
        Objects.requireNonNull(documentId, "documentId");
        try {
            byte[] iv = new byte[IV_LENGTH_BYTES];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            cipher.updateAAD(documentId.getBytes(StandardCharsets.UTF_8));
            byte[] ciphertext = cipher.doFinal(plaintext);

            return new EncryptionEnvelope(
                    ALGORITHM_ID,
                    keyId,
                    Base64.getEncoder().encodeToString(iv),
                    Base64.getEncoder().encodeToString(ciphertext));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to encrypt payload", e);
        }
    }

    public record EncryptionEnvelope(String alg, String kid, String iv, String ct) {
    }
}
