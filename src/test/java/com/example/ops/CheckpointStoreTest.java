package com.example.ops;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void loadMissingFile_returnsEmpty() {
        CheckpointStore store = new CheckpointStore(tempDir.resolve("missing.json"));
        assertTrue(store.load().isEmpty());
    }

    @Test
    void saveThenLoad_restoresLastWrittenValues() {
        Path path = tempDir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(path);
        CheckpointStore.Checkpoint checkpoint = new CheckpointStore.Checkpoint("doc-1", 10, 2, 1, 0);

        store.save(checkpoint);
        Optional<CheckpointStore.Checkpoint> loaded = store.load();

        assertTrue(loaded.isPresent());
        assertEquals(checkpoint, loaded.get());
    }

    @Test
    void saveIsAtomicEnoughForConcurrentReaders_neverSeesCorruptJson() throws Exception {
        Path path = tempDir.resolve("checkpoint.json");
        CheckpointStore store = new CheckpointStore(path);
        store.save(new CheckpointStore.Checkpoint("doc-0", 0, 0, 0, 0));

        int writes = 100;
        int reads = 1000;

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        var exec = Executors.newFixedThreadPool(2);
        exec.submit(() -> {
            await(start);
            for (int i = 1; i <= writes; i++) {
                store.save(new CheckpointStore.Checkpoint("doc-" + i, i, i, i, 0));
            }
            done.countDown();
        });
        exec.submit(() -> {
            await(start);
            for (int i = 0; i < reads; i++) {
                assertDoesNotThrow(() -> {
                    Optional<CheckpointStore.Checkpoint> cp = store.load();
                    assertTrue(cp.isPresent());
                    assertNotNull(cp.get().lastSuccessfulDocId());
                    assertTrue(cp.get().scanned() >= 0);
                });
            }
            done.countDown();
        });

        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        exec.shutdownNow();

        assertTrue(Files.exists(path));
        assertFalse(Files.readString(path).isBlank());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
