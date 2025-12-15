package com.example.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;

class EncryptorTest {
    @Test
    void encryptsSuccessfully_withAes256GcmEnvelope() {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        Encryptor.EncryptionEnvelope env = encryptor.encrypt("hello".getBytes(StandardCharsets.UTF_8), "doc-1");
        assertEquals("AES-256-GCM", env.alg());
        assertEquals("kid-1", env.kid());
        assertNotNull(env.iv());
        assertNotNull(env.ct());
        assertTrue(!env.iv().isBlank());
        assertTrue(!env.ct().isBlank());
    }

    @Test
    void samePlaintextSameDocId_producesDifferentCiphertext_dueToRandomIv() {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        byte[] plaintext = "same".getBytes(StandardCharsets.UTF_8);
        Encryptor.EncryptionEnvelope a = encryptor.encrypt(plaintext, "doc-1");
        Encryptor.EncryptionEnvelope b = encryptor.encrypt(plaintext, "doc-1");

        assertNotEquals(a.iv(), b.iv());
        assertNotEquals(a.ct(), b.ct());
    }

    @Test
    void decryptsToOriginalPlaintext_usingSameKeyIvAndDocId() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        byte[] plaintext = "payload".getBytes(StandardCharsets.UTF_8);
        Encryptor.EncryptionEnvelope env = encryptor.encrypt(plaintext, "doc-99");

        byte[] roundTrip = CryptoTestSupport.decryptAes256Gcm(env.iv(), env.ct(), key, "doc-99");
        assertArrayEquals(plaintext, roundTrip);
    }

    @Test
    void wrongAadDocId_failsDecryption() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        Encryptor.EncryptionEnvelope env = encryptor.encrypt("payload".getBytes(StandardCharsets.UTF_8), "doc-good");
        assertThrows(AEADBadTagException.class, () -> CryptoTestSupport.decryptAes256Gcm(env.iv(), env.ct(), key, "doc-bad"));
    }

    @Test
    void emptyPlaintext_encryptsAndDecrypts() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        Encryptor.EncryptionEnvelope env = encryptor.encrypt(new byte[0], "doc-1");
        byte[] roundTrip = CryptoTestSupport.decryptAes256Gcm(env.iv(), env.ct(), key, "doc-1");
        assertArrayEquals(new byte[0], roundTrip);
    }

    @Test
    void nullPlaintext_isRejectedSafely() {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());

        assertThrows(NullPointerException.class, () -> encryptor.encrypt(null, "doc-1"));
    }
}
