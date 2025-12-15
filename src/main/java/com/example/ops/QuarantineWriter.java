package com.example.ops;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

public final class QuarantineWriter {
    private static final int DEFAULT_MAX_EXCEPTION_CHARS = 64 * 1024;
    private static final int MAX_CAUSE_DEPTH = 8;

    private final Path quarantineDir;
    private final int maxExceptionChars;

    public QuarantineWriter(Path quarantineDir) {
        this(quarantineDir, DEFAULT_MAX_EXCEPTION_CHARS);
    }

    public QuarantineWriter(Path quarantineDir, int maxExceptionChars) {
        this.quarantineDir = Objects.requireNonNull(quarantineDir, "quarantineDir");
        if (maxExceptionChars <= 0) {
            throw new IllegalArgumentException("maxExceptionChars must be > 0");
        }
        this.maxExceptionChars = maxExceptionChars;
    }

    public Path write(String docId, String stage, Throwable exception) {
        docId = requireNotBlank(docId, "docId");
        stage = requireNotBlank(stage, "stage");
        Objects.requireNonNull(exception, "exception");

        try {
            Files.createDirectories(quarantineDir);

            String payload = buildPayload(docId, stage, exception);
            String baseName = Instant.now().toEpochMilli()
                    + "-" + stageSlug(stage)
                    + "-" + shortHash(docId);

            IOException last = null;
            for (int i = 0; i < 5; i++) {
                Path path = quarantineDir.resolve(baseName + (i == 0 ? "" : "-" + i) + ".txt");
                try {
                    return Files.writeString(
                            path,
                            payload,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE);
                } catch (IOException e) {
                    last = e;
                }
            }
            throw new IllegalStateException("Unable to write quarantine entry after retries", last);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write quarantine entry", e);
        }
    }

    private String buildPayload(String docId, String stage, Throwable exception) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("docId=").append(docId).append('\n');
        sb.append("stage=").append(stage).append('\n');
        appendExceptionChain(sb, exception);
        return sb.toString();
    }

    private void appendExceptionChain(StringBuilder sb, Throwable exception) {
        int depth = 0;
        Throwable current = exception;
        while (current != null && depth < MAX_CAUSE_DEPTH) {
            String prefix = depth == 0 ? "exception" : "cause" + depth;
            sb.append(prefix).append("Class=").append(current.getClass().getName()).append('\n');
            sb.append(prefix).append("MessagePresent=").append(current.getMessage() != null && !current.getMessage().isBlank()).append('\n');
            sb.append(prefix).append("MessageSha256=").append(sha256Hex(safe(current.getMessage()))).append('\n');
            current = current.getCause();
            depth++;
        }
        if (current != null) {
            sb.append("causeTruncated=true\n");
        }

        if (sb.length() > maxExceptionChars) {
            sb.setLength(maxExceptionChars);
            sb.append("\n... truncated ...\n");
        }
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static String requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static String stageSlug(String stage) {
        StringBuilder sb = new StringBuilder(stage.length());
        for (int i = 0; i < stage.length(); i++) {
            char c = stage.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        String out = sb.toString();
        return out.isBlank() ? "stage" : out;
    }

    private static String shortHash(String docId) {
        byte[] digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            digest = md.digest(docId.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        StringBuilder hex = new StringBuilder(16);
        for (int i = 0; i < 8; i++) {
            hex.append(Character.forDigit((digest[i] >>> 4) & 0xF, 16));
            hex.append(Character.forDigit(digest[i] & 0xF, 16));
        }
        return hex.toString();
    }

    private static String sha256Hex(String value) {
        byte[] digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        StringBuilder hex = new StringBuilder(64);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >>> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
