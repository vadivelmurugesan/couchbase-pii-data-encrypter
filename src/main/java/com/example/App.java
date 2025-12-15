package com.example;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.example.couchbase.CouchbaseClients;
import com.example.crypto.Encryptor;
import com.example.crypto.KeyScanPiiEncryptor;
import com.example.migrate.MigrationJob;
import com.example.ops.CheckpointStore;
import com.example.ops.KillSwitch;
import com.example.ops.QuarantineWriter;
import com.example.ops.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Instant startedAt = Instant.now();
        String runId = UUID.randomUUID().toString();

        try {
            Properties props = loadProperties(resolvePropertiesPath());
            PiiConfig piiConfig = validateStartupConfig(props);

            KeystoreConfig keystoreConfig = KeystoreConfig.fromEnv();
            SecretKey secretKey = loadSecretKey(keystoreConfig);
            String keyId = keystoreConfig.keyId();

            Encryptor encryptor = new Encryptor(secretKey, keyId);
            KeyScanPiiEncryptor piiEncryptor = new KeyScanPiiEncryptor(
                    encryptor,
                    piiConfig.keys(),
                    piiConfig.regex());

            CouchbaseClients.EnvironmentConfig env = new CouchbaseClients.EnvironmentConfig(
                    parseDuration(props, "couchbase.kvTimeout", Duration.ofSeconds(2)),
                    parseDuration(props, "couchbase.connectTimeout", Duration.ofSeconds(10)),
                    parseInt(props, "couchbase.numKvConnections", 2),
                    parseDuration(props, "couchbase.waitUntilReadyTimeout", Duration.ofSeconds(30)),
                    parseDuration(props, "couchbase.shutdownTimeout", Duration.ofSeconds(10)));

            CouchbaseClients.ClusterConfig sourceCfg = new CouchbaseClients.ClusterConfig(
                    requireProperty(props, "source.couchbase.connectionString"),
                    requirePropertyOrEnv(props, "source.couchbase.username", "SOURCE_COUCHBASE_USERNAME"),
                    requirePropertyOrEnv(props, "source.couchbase.password", "SOURCE_COUCHBASE_PASSWORD"),
                    requireProperty(props, "source.couchbase.bucket"),
                    requireProperty(props, "source.couchbase.scope"),
                    requireProperty(props, "source.couchbase.collection"));

            CouchbaseClients.ClusterConfig destCfg = new CouchbaseClients.ClusterConfig(
                    requireProperty(props, "destination.couchbase.connectionString"),
                    requirePropertyOrEnv(props, "destination.couchbase.username", "DESTINATION_COUCHBASE_USERNAME"),
                    requirePropertyOrEnv(props, "destination.couchbase.password", "DESTINATION_COUCHBASE_PASSWORD"),
                    requireProperty(props, "destination.couchbase.bucket"),
                    requireProperty(props, "destination.couchbase.scope"),
                    requireProperty(props, "destination.couchbase.collection"));

            RateLimiter rateLimiter = createRateLimiter(props);
            CheckpointStore checkpointStore = new CheckpointStore(Path.of(requireProperty(props, "migration.checkpoint.path")));
            QuarantineWriter quarantineWriter = new QuarantineWriter(Path.of(requireProperty(props, "migration.quarantine.path")));

            boolean killSwitchEnabled = parseBoolean(props, "migration.killSwitch.enabled", false);
            KillSwitch killSwitch = new KillSwitch(Path.of(requireProperty(props, "migration.killSwitch.path")), killSwitchEnabled);

            boolean dryRun = parseBoolean(props, "migration.dryRun", true);
            int maxInFlight = parseInt(props, "migration.concurrency.max", 32);
            int checkpointEvery = parseInt(props, "migration.checkpoint.every", 1000);
            DurabilityLevel durability = parseDurability(props);

            String configChecksum = configChecksum(props, keyId, durability, dryRun, maxInFlight, checkpointEvery);

            Map<String, Object> audit = new LinkedHashMap<>();
            audit.put("runId", runId);
            audit.put("startedAt", startedAt.toString());
            audit.put("configChecksum", configChecksum);
            audit.put("keyId", keyId);
            audit.put("durability", durability.toString());
            audit.put("dryRun", dryRun);

            try (CouchbaseClients clients = CouchbaseClients.connect(env, sourceCfg, destCfg)) {
                MigrationJob job = new MigrationJob(
                        clients.sourceReactiveCollection(),
                        clients.destinationReactiveCollection(),
                        piiEncryptor,
                        rateLimiter,
                        checkpointStore,
                        quarantineWriter,
                        killSwitch,
                        new MigrationJob.Config(maxInFlight, checkpointEvery, dryRun, durability));

                CheckpointStore.Checkpoint finalCheckpoint = job.run().block();
                if (finalCheckpoint == null) {
                    throw new IllegalStateException("Migration completed without checkpoint");
                }

                audit.put("endedAt", Instant.now().toString());
                audit.put("counts", Map.of(
                        "scanned", finalCheckpoint.scanned(),
                        "encrypted", finalCheckpoint.encrypted(),
                        "written", finalCheckpoint.written(),
                        "quarantined", finalCheckpoint.quarantined()));
            }

            writeAudit(props, runId, audit);
        } catch (Exception e) {
            log.error(
                    "Migration run failed (runId={}, exceptionClass={}, exceptionMessageSha256={})",
                    runId,
                    e.getClass().getName(),
                    sha256Base64(safe(e.getMessage())));
            System.exit(1);
        }
    }

    private static Path resolvePropertiesPath() {
        String explicit = System.getProperty("app.properties");
        if (explicit != null && !explicit.isBlank()) {
            return Path.of(explicit);
        }
        return Path.of("application.properties");
    }

    private static Properties loadProperties(Path path) throws IOException {
        Properties props = new Properties();
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                props.load(in);
            }
            return props;
        }
        try (InputStream in = App.class.getResourceAsStream("/application.properties")) {
            if (in == null) {
                throw new IllegalStateException("Unable to find application.properties at " + path + " or on classpath");
            }
            props.load(in);
            return props;
        }
    }

    private static void enforceSourceWritesFrozen(Properties props) {
        if (!parseBoolean(props, "sourceWritesFrozen", false)) {
            throw new IllegalStateException("Refusing to run: sourceWritesFrozen must be true");
        }
    }

    static PiiConfig validateStartupConfig(Properties props) {
        Objects.requireNonNull(props, "props");
        enforceSourceWritesFrozen(props);

        List<String> piiKeys = parseCsvList(props.getProperty("pii.keys"));
        Pattern piiRegex = compileOptionalRegex(props.getProperty("pii.keyRegex"));
        if (piiKeys.isEmpty() && piiRegex == null) {
            throw new IllegalArgumentException("Refusing to run: configure pii.keys and/or pii.keyRegex");
        }

        requireProperty(props, "source.couchbase.connectionString");
        requireProperty(props, "source.couchbase.bucket");
        requireProperty(props, "source.couchbase.scope");
        requireProperty(props, "source.couchbase.collection");
        requirePropertyOrEnv(props, "source.couchbase.username", "SOURCE_COUCHBASE_USERNAME");
        requirePropertyOrEnv(props, "source.couchbase.password", "SOURCE_COUCHBASE_PASSWORD");

        requireProperty(props, "destination.couchbase.connectionString");
        requireProperty(props, "destination.couchbase.bucket");
        requireProperty(props, "destination.couchbase.scope");
        requireProperty(props, "destination.couchbase.collection");
        requirePropertyOrEnv(props, "destination.couchbase.username", "DESTINATION_COUCHBASE_USERNAME");
        requirePropertyOrEnv(props, "destination.couchbase.password", "DESTINATION_COUCHBASE_PASSWORD");

        requireProperty(props, "migration.checkpoint.path");
        requireProperty(props, "migration.quarantine.path");
        requireProperty(props, "migration.killSwitch.path");
        requireProperty(props, "migration.durability");
        parseDurability(props);

        return new PiiConfig(piiKeys, piiRegex);
    }

    private static RateLimiter createRateLimiter(Properties props) {
        double permits = parseDouble(props, "migration.rateLimit.permitsPerSecond", 0d);
        if (!(permits > 0d)) {
            return RateLimiter.unlimited();
        }
        return RateLimiter.create(permits);
    }

    private static Duration parseDuration(Properties props, String key, Duration defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String v = raw.trim();
        if (v.matches("^\\d+$")) {
            return Duration.ofMillis(Long.parseLong(v));
        }
        return Duration.parse(v);
    }

    private static int parseInt(Properties props, String key, int defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(raw.trim());
    }

    private static double parseDouble(Properties props, String key, double defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Double.parseDouble(raw.trim());
    }

    private static boolean parseBoolean(Properties props, String key, boolean defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private static String requireProperty(Properties props, String key) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return raw.trim();
    }

    private static String requirePropertyOrEnv(Properties props, String propertyKey, String envKey) {
        String prop = props.getProperty(propertyKey);
        if (prop != null && !prop.isBlank()) {
            return prop.trim();
        }
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        throw new IllegalArgumentException("Missing required setting: " + propertyKey + " (or env " + envKey + ")");
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static List<String> parseCsvList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        String[] parts = raw.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            String v = p.trim();
            if (!v.isEmpty()) {
                out.add(v);
            }
        }
        return List.copyOf(out);
    }

    private static Pattern compileOptionalRegex(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return Pattern.compile(raw.trim(), Pattern.CASE_INSENSITIVE);
    }

    private static SecretKey loadSecretKey(KeystoreConfig cfg) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(cfg.type());
        try (InputStream in = Files.newInputStream(cfg.path())) {
            keyStore.load(in, cfg.storePassword().toCharArray());
        }

        char[] keyPassword = cfg.keyPassword() != null ? cfg.keyPassword().toCharArray() : cfg.storePassword().toCharArray();
        Key key = keyStore.getKey(cfg.alias(), keyPassword);
        if (!(key instanceof SecretKey secretKey)) {
            throw new IllegalStateException("Keystore alias is not a SecretKey: " + cfg.alias());
        }
        if (!"AES".equalsIgnoreCase(secretKey.getAlgorithm())) {
            throw new IllegalStateException("SecretKey algorithm must be AES");
        }
        byte[] encoded = secretKey.getEncoded();
        if (encoded == null || encoded.length != 32) {
            throw new IllegalStateException("SecretKey must be 256-bit (32 bytes)");
        }
        return secretKey;
    }

    private static void writeAudit(Properties props, String runId, Map<String, Object> audit) {
        String dir = props.getProperty("migration.audit.dir", "audit");
        Path auditDir = Path.of(dir);
        try {
            Files.createDirectories(auditDir);
            ObjectMapper mapper = new ObjectMapper();
            byte[] payload = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(audit);

            Path tmp = auditDir.resolve("audit-" + runId + ".tmp");
            Path out = auditDir.resolve("audit-" + runId + ".json");
            Files.write(tmp, payload);
            Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to write audit file", e);
        }
    }

    private static String configChecksum(
            Properties props,
            String keyId,
            DurabilityLevel durability,
            boolean dryRun,
            int maxInFlight,
            int checkpointEvery) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            update(md, "source.couchbase.connectionString", props.getProperty("source.couchbase.connectionString"));
            update(md, "source.couchbase.bucket", props.getProperty("source.couchbase.bucket"));
            update(md, "source.couchbase.scope", props.getProperty("source.couchbase.scope"));
            update(md, "source.couchbase.collection", props.getProperty("source.couchbase.collection"));
            update(md, "destination.couchbase.connectionString", props.getProperty("destination.couchbase.connectionString"));
            update(md, "destination.couchbase.bucket", props.getProperty("destination.couchbase.bucket"));
            update(md, "destination.couchbase.scope", props.getProperty("destination.couchbase.scope"));
            update(md, "destination.couchbase.collection", props.getProperty("destination.couchbase.collection"));
            update(md, "couchbase.kvTimeout", props.getProperty("couchbase.kvTimeout"));
            update(md, "couchbase.connectTimeout", props.getProperty("couchbase.connectTimeout"));
            update(md, "couchbase.numKvConnections", props.getProperty("couchbase.numKvConnections"));
            update(md, "pii.keys", props.getProperty("pii.keys"));
            update(md, "pii.keyRegex", props.getProperty("pii.keyRegex"));
            update(md, "migration.rateLimit.permitsPerSecond", props.getProperty("migration.rateLimit.permitsPerSecond"));
            update(md, "migration.concurrency.max", String.valueOf(maxInFlight));
            update(md, "migration.checkpoint.every", String.valueOf(checkpointEvery));
            update(md, "migration.checkpoint.path", props.getProperty("migration.checkpoint.path"));
            update(md, "migration.quarantine.path", props.getProperty("migration.quarantine.path"));
            update(md, "migration.killSwitch.enabled", props.getProperty("migration.killSwitch.enabled"));
            update(md, "migration.killSwitch.path", props.getProperty("migration.killSwitch.path"));
            update(md, "migration.durability", durability.toString());
            update(md, "migration.dryRun", String.valueOf(dryRun));
            update(md, "keyId", keyId);

            return Base64.getEncoder().encodeToString(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void update(MessageDigest md, String key, String value) {
        md.update(key.getBytes(StandardCharsets.UTF_8));
        md.update((byte) 0);
        if (value != null) {
            md.update(value.trim().getBytes(StandardCharsets.UTF_8));
        }
        md.update((byte) 0);
    }

    private static String sha256Base64(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private record KeystoreConfig(Path path, String type, String storePassword, String alias, String keyPassword, String keyId) {
        static KeystoreConfig fromEnv() {
            String path = requireEnv("KEYSTORE_PATH");
            String type = Optional.ofNullable(System.getenv("KEYSTORE_TYPE")).filter(s -> !s.isBlank()).orElse("PKCS12");
            String storePassword = requireEnv("KEYSTORE_PASSWORD");
            String alias = requireEnv("KEYSTORE_ALIAS");
            String keyPassword = Optional.ofNullable(System.getenv("KEYSTORE_KEY_PASSWORD")).filter(s -> !s.isBlank()).orElse(null);
            String keyId = Optional.ofNullable(System.getenv("KEY_ID")).filter(s -> !s.isBlank()).orElse(alias);
            return new KeystoreConfig(Path.of(path), type, storePassword, alias, keyPassword, keyId);
        }

        private static String requireEnv(String name) {
            String v = System.getenv(name);
            if (v == null || v.isBlank()) {
                throw new IllegalArgumentException("Missing required env var: " + name);
            }
            return v.trim();
        }
    }

    record PiiConfig(List<String> keys, Pattern regex) {
        PiiConfig {
            keys = List.copyOf(Objects.requireNonNull(keys, "keys"));
        }
    }

    private static DurabilityLevel parseDurability(Properties props) {
        String raw = requireProperty(props, "migration.durability");
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        try {
            return DurabilityLevel.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid migration.durability: " + raw
                            + " (expected one of NONE, MAJORITY, MAJORITY_AND_PERSIST_TO_ACTIVE, PERSIST_TO_MAJORITY)");
        }
    }
}
