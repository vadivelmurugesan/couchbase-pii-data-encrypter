package com.example.ops;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class CheckpointStore {
    private final Path checkpointPath;
    private final ObjectMapper mapper;

    public CheckpointStore(Path checkpointPath) {
        this(checkpointPath, new ObjectMapper());
    }

    public CheckpointStore(Path checkpointPath, ObjectMapper mapper) {
        this.checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public Optional<Checkpoint> load() {
        if (!Files.exists(checkpointPath)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(checkpointPath);
            if (bytes.length == 0) {
                return Optional.empty();
            }
            return Optional.of(mapper.readValue(bytes, Checkpoint.class));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read checkpoint: " + checkpointPath, e);
        }
    }

    public void save(Checkpoint checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        try {
            Path parent = checkpointPath.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            byte[] payload = mapper.writeValueAsBytes(checkpoint);
            Path temp = checkpointPath.resolveSibling(
                    checkpointPath.getFileName() + ".tmp-" + UUID.randomUUID());

            try (FileChannel channel = FileChannel.open(
                    temp,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            try {
                Files.move(
                        temp,
                        checkpointPath,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException ignored) {
                }
                throw new IllegalStateException("Atomic move not supported for checkpoint: " + checkpointPath, e);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write checkpoint: " + checkpointPath, e);
        }
    }

    public record Checkpoint(
            String lastSuccessfulDocId,
            long scanned,
            long encrypted,
            long written,
            long quarantined) {
        public Checkpoint {
            if (lastSuccessfulDocId != null && lastSuccessfulDocId.isBlank()) {
                lastSuccessfulDocId = null;
            }
            if (scanned < 0 || encrypted < 0 || written < 0 || quarantined < 0) {
                throw new IllegalArgumentException("Counters must be >= 0");
            }
        }
    }
}
