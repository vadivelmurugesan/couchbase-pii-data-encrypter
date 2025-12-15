package com.example.ops;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class KillSwitch {
    private final Path file;
    private final boolean enabled;

    public KillSwitch(Path file) {
        this(file, true);
    }

    public KillSwitch(Path file, boolean enabled) {
        this.file = Objects.requireNonNull(file, "file");
        this.enabled = enabled;
    }

    public boolean engaged() {
        return enabled && Files.exists(file);
    }

    public void throwIfEngaged() {
        if (engaged()) {
            throw new EngagedException(file);
        }
    }

    public static final class EngagedException extends RuntimeException {
        public EngagedException(Path file) {
            super("Kill switch engaged (file exists): " + file);
        }
    }
}
