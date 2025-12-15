package com.example.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import org.junit.jupiter.api.Test;

class KeyScanPiiEncryptorTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void encryptsMatchingKeysCaseInsensitive_andLeavesNonPiiUntouched() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());
        KeyScanPiiEncryptor piiEncryptor = new KeyScanPiiEncryptor(encryptor, List.of("ssn"), null, mapper);

        String json = """
                {
                  "SSN": "123-45-6789",
                  "profile": { "city": "NY" }
                }
                """;
        String docId = "doc-1";
        byte[] out = piiEncryptor.encrypt(json.getBytes(StandardCharsets.UTF_8), docId);

        JsonNode root = mapper.readTree(out);
        assertTrue(root.get("SSN").has("_enc"));
        assertEquals("NY", root.get("profile").get("city").asText());
        assertFalse(new String(out, StandardCharsets.UTF_8).contains("123-45-6789"));
    }

    @Test
    void encryptsMatchingKeysInNestedObjectsArraysAndMixedDepth() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());
        KeyScanPiiEncryptor piiEncryptor = new KeyScanPiiEncryptor(
                encryptor,
                List.of("ssn", "email"),
                Pattern.compile(".*secret.*", Pattern.CASE_INSENSITIVE),
                mapper);

        String json = """
                {
                  "users": [
                    {
                      "name": "alice",
                      "SSN": "111-22-3333",
                      "contact": { "Email": "a@example.com" },
                      "metadata": [
                        { "secretCode": "abc" },
                        { "nested": [ { "secretToken": { "k": 1 } } ] }
                      ]
                    }
                  ],
                  "public": { "v": 1 }
                }
                """;
        String docId = "doc-2";
        byte[] out = piiEncryptor.encrypt(json.getBytes(StandardCharsets.UTF_8), docId);
        JsonNode root = mapper.readTree(out);

        assertEncryptedEnvelopeShape(root.at("/users/0/SSN"));
        assertEncryptedEnvelopeShape(root.at("/users/0/contact/Email"));
        assertEncryptedEnvelopeShape(root.at("/users/0/metadata/0/secretCode"));
        assertEncryptedEnvelopeShape(root.at("/users/0/metadata/1/nested/0/secretToken"));
        assertEquals(1, root.at("/public/v").asInt());
    }

    @Test
    void alreadyEncryptedFieldsAreSkipped_idempotent() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());
        KeyScanPiiEncryptor piiEncryptor = new KeyScanPiiEncryptor(encryptor, List.of("ssn"), null, mapper);

        String json = """
                {
                  "profile": {
                    "ssn": {
                      "_enc": { "alg": "legacy", "kid": "legacy", "iv": "iv", "ct": "ct" }
                    },
                    "SSN2": "should-not-match"
                  }
                }
                """;
        String docId = "doc-3";
        byte[] out = piiEncryptor.encrypt(json.getBytes(StandardCharsets.UTF_8), docId);
        JsonNode root = mapper.readTree(out);
        assertEquals("legacy", root.at("/profile/ssn/_enc/alg").asText());
        assertEquals("should-not-match", root.at("/profile/SSN2").asText());
    }

    @Test
    void goldenJson_encryptsAllIntendedFields_andPreservesNonPiiFields() throws Exception {
        SecretKey key = CryptoTestSupport.deterministicAes256Key();
        Encryptor encryptor = new Encryptor(key, "kid-1", CryptoTestSupport.deterministicSecureRandom());
        KeyScanPiiEncryptor piiEncryptor = new KeyScanPiiEncryptor(
                encryptor,
                List.of("ssn", "email", "phone"),
                Pattern.compile(".*secret.*", Pattern.CASE_INSENSITIVE),
                mapper);

        String json = """
                {
                  "tenant": "t1",
                  "profile": {
                    "name": "Bob",
                    "ssn": "999-88-7777",
                    "email": "bob@example.com",
                    "address": { "city": "NY", "zip": 10001 }
                  },
                  "accounts": [
                    {
                      "type": "checking",
                      "phone": "+1-555-0000",
                      "notes": [ "a", "b" ],
                      "meta": { "secretCode": "abc" }
                    },
                    {
                      "type": "savings",
                      "meta": { "secretToken": { "k": 1, "v": true } }
                    }
                  ]
                }
                """;
        String docId = "doc-golden";
        JsonNode original = mapper.readTree(json);

        byte[] out = piiEncryptor.encrypt(json.getBytes(StandardCharsets.UTF_8), docId);
        JsonNode encrypted = mapper.readTree(out);

        assertEquals("t1", encrypted.get("tenant").asText());
        assertEquals(original.at("/profile/address"), encrypted.at("/profile/address"));
        assertEquals(original.at("/accounts/0/type"), encrypted.at("/accounts/0/type"));
        assertEquals(original.at("/accounts/0/notes"), encrypted.at("/accounts/0/notes"));

        assertEncryptedAndDecryptsToOriginal(encrypted.at("/profile/ssn"), original.at("/profile/ssn"), key, docId);
        assertEncryptedAndDecryptsToOriginal(encrypted.at("/profile/email"), original.at("/profile/email"), key, docId);
        assertEncryptedAndDecryptsToOriginal(encrypted.at("/accounts/0/phone"), original.at("/accounts/0/phone"), key, docId);
        assertEncryptedAndDecryptsToOriginal(encrypted.at("/accounts/0/meta/secretCode"), original.at("/accounts/0/meta/secretCode"), key, docId);
        assertEncryptedAndDecryptsToOriginal(encrypted.at("/accounts/1/meta/secretToken"), original.at("/accounts/1/meta/secretToken"), key, docId);

        String outText = new String(out, StandardCharsets.UTF_8);
        for (String piiValue : List.of("999-88-7777", "bob@example.com", "+1-555-0000", "\"abc\"")) {
            assertFalse(outText.contains(piiValue), "output must not contain plaintext: " + piiValue);
        }
    }

    private void assertEncryptedAndDecryptsToOriginal(JsonNode wrapper, JsonNode originalValue, SecretKey key, String docId) throws Exception {
        assertEncryptedEnvelopeShape(wrapper);
        ObjectNode enc = (ObjectNode) wrapper.get("_enc");
        byte[] plaintextJson = CryptoTestSupport.decryptAes256Gcm(
                enc.get("iv").asText(),
                enc.get("ct").asText(),
                key,
                docId);
        JsonNode decrypted = mapper.readTree(plaintextJson);
        assertEquals(originalValue, decrypted);
    }

    private static void assertEncryptedEnvelopeShape(JsonNode wrapper) {
        assertTrue(wrapper.isObject(), "encrypted field must be an object wrapper");
        ObjectNode obj = (ObjectNode) wrapper;
        assertEquals(Set.of("v", "_enc"), fieldNameSet(obj), "wrapper must contain only v and _enc");
        assertEquals(1, obj.get("v").asInt());
        assertNotNull(obj.get("_enc"));
        assertTrue(obj.get("_enc").isObject());

        ObjectNode enc = (ObjectNode) obj.get("_enc");
        assertEquals(Set.of("alg", "kid", "iv", "ct"), fieldNameSet(enc), "_enc must contain only alg,kid,iv,ct");
        assertFalse(enc.get("alg").asText().isBlank());
        assertFalse(enc.get("kid").asText().isBlank());
        assertFalse(enc.get("iv").asText().isBlank());
        assertFalse(enc.get("ct").asText().isBlank());

        Base64.getDecoder().decode(enc.get("iv").asText());
        Base64.getDecoder().decode(enc.get("ct").asText());
    }

    private static Set<String> fieldNameSet(ObjectNode obj) {
        java.util.HashSet<String> out = new java.util.HashSet<>();
        obj.fieldNames().forEachRemaining(out::add);
        return out;
    }
}
