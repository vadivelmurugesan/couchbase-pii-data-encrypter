package com.example;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class AppConfigSafetyTest {
    @Test
    void sourceWritesFrozenFalse_causesStartupAbort() {
        Properties props = baselineProps();
        props.setProperty("sourceWritesFrozen", "false");
        assertThrows(IllegalStateException.class, () -> App.validateStartupConfig(props));
    }

    @Test
    void missingRequiredConfigKeys_failFast() {
        Properties props = baselineProps();
        props.remove("source.couchbase.connectionString");
        assertThrows(IllegalArgumentException.class, () -> App.validateStartupConfig(props));
    }

    @Test
    void emptyPiiKeysAndEmptyRegex_causesFailure() {
        Properties props = baselineProps();
        props.setProperty("pii.keys", "");
        props.setProperty("pii.keyRegex", "");
        assertThrows(IllegalArgumentException.class, () -> App.validateStartupConfig(props));
    }

    @Test
    void invalidDurabilityValue_failsFast() {
        Properties props = baselineProps();
        props.setProperty("migration.durability", "NOT_A_REAL_LEVEL");
        assertThrows(IllegalArgumentException.class, () -> App.validateStartupConfig(props));
    }

    private static Properties baselineProps() {
        Properties props = new Properties();
        props.setProperty("sourceWritesFrozen", "true");

        props.setProperty("pii.keys", "ssn");
        props.setProperty("pii.keyRegex", "");

        props.setProperty("source.couchbase.connectionString", "couchbase://127.0.0.1");
        props.setProperty("source.couchbase.bucket", "b");
        props.setProperty("source.couchbase.scope", "_default");
        props.setProperty("source.couchbase.collection", "_default");
        props.setProperty("source.couchbase.username", "u");
        props.setProperty("source.couchbase.password", "p");

        props.setProperty("destination.couchbase.connectionString", "couchbase://127.0.0.2");
        props.setProperty("destination.couchbase.bucket", "b2");
        props.setProperty("destination.couchbase.scope", "_default");
        props.setProperty("destination.couchbase.collection", "_default");
        props.setProperty("destination.couchbase.username", "u2");
        props.setProperty("destination.couchbase.password", "p2");

        props.setProperty("migration.checkpoint.path", "checkpoints/checkpoint.dat");
        props.setProperty("migration.quarantine.path", "quarantine");
        props.setProperty("migration.killSwitch.path", "kill.switch");
        props.setProperty("migration.durability", "NONE");
        return props;
    }
}
