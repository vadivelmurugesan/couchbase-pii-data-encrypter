package com.example.crypto;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

final class CryptoTestSupport {
    private CryptoTestSupport() {
    }

    static SecretKey deterministicAes256Key() {
        byte[] keyBytes = new byte[32];
        for (int i = 0; i < keyBytes.length; i++) {
            keyBytes[i] = (byte) (i * 7 + 3);
        }
        return new javax.crypto.spec.SecretKeySpec(keyBytes, "AES");
    }

    static SecureRandom deterministicSecureRandom() {
        return new SecureRandom() {
            private long counter = 1;

            @Override
            public void nextBytes(byte[] bytes) {
                long x = counter++;
                for (int i = 0; i < bytes.length; i++) {
                    x ^= (x << 13);
                    x ^= (x >>> 7);
                    x ^= (x << 17);
                    bytes[i] = (byte) x;
                }
            }
        };
    }

    static byte[] decryptAes256Gcm(String ivB64, String ctB64, SecretKey key, String docId) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        byte[] iv = Base64.getDecoder().decode(ivB64);
        byte[] ct = Base64.getDecoder().decode(ctB64);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, iv));
        cipher.updateAAD(docId.getBytes(StandardCharsets.UTF_8));
        return cipher.doFinal(ct);
    }
}

