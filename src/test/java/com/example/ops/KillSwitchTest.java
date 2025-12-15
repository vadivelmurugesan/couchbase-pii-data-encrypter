package com.example.ops;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KillSwitchTest {
    @TempDir
    Path tempDir;

    @Test
    void engagedReflectsFileExistence() throws Exception {
        Path stopFile = tempDir.resolve("stop");
        KillSwitch killSwitch = new KillSwitch(stopFile, true);

        assertFalse(killSwitch.engaged());
        Files.writeString(stopFile, "x");
        assertTrue(killSwitch.engaged());
    }

    @Test
    void disabledKillSwitchNeverEngages() throws Exception {
        Path stopFile = tempDir.resolve("stop");
        Files.writeString(stopFile, "x");
        KillSwitch killSwitch = new KillSwitch(stopFile, false);
        assertFalse(killSwitch.engaged());
    }

    @Test
    void throwIfEngagedIsSafeWhenNotEngaged() {
        KillSwitch killSwitch = new KillSwitch(tempDir.resolve("missing"), true);
        killSwitch.throwIfEngaged();
    }

    @Test
    void throwIfEngagedThrowsWhenEngaged() throws Exception {
        Path stopFile = tempDir.resolve("stop");
        Files.writeString(stopFile, "x");
        KillSwitch killSwitch = new KillSwitch(stopFile, true);
        assertThrows(KillSwitch.EngagedException.class, killSwitch::throwIfEngaged);
    }
}

