package com.example.migrate;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanTerm;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.kv.UpsertOptions;
import com.example.crypto.KeyScanPiiEncryptor;
import com.example.ops.CheckpointStore;
import com.example.ops.KillSwitch;
import com.example.ops.QuarantineWriter;
import com.example.ops.RateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

public final class MigrationJob {
    private static final Logger log = LoggerFactory.getLogger(MigrationJob.class);

    private final ReactiveCollection source;
    private final ReactiveCollection destination;
    private final KeyScanPiiEncryptor piiEncryptor;
    private final RateLimiter rateLimiter;
    private final CheckpointStore checkpointStore;
    private final QuarantineWriter quarantineWriter;
    private final KillSwitch killSwitch;
    private final Config config;

    public MigrationJob(
            ReactiveCollection source,
            ReactiveCollection destination,
            KeyScanPiiEncryptor piiEncryptor,
            RateLimiter rateLimiter,
            CheckpointStore checkpointStore,
            QuarantineWriter quarantineWriter,
            KillSwitch killSwitch,
            Config config) {
        this.source = Objects.requireNonNull(source, "source");
        this.destination = Objects.requireNonNull(destination, "destination");
        this.piiEncryptor = Objects.requireNonNull(piiEncryptor, "piiEncryptor");
        this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter");
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStore");
        this.quarantineWriter = Objects.requireNonNull(quarantineWriter, "quarantineWriter");
        this.killSwitch = Objects.requireNonNull(killSwitch, "killSwitch");
        this.config = Objects.requireNonNull(config, "config");
    }

    public Mono<CheckpointStore.Checkpoint> run() {
        Optional<CheckpointStore.Checkpoint> loaded = checkpointStore.load();
        AtomicReference<String> resumeAfterDocId = new AtomicReference<>(loaded.map(CheckpointStore.Checkpoint::lastSuccessfulDocId).orElse(null));
        AtomicLong scanned = new AtomicLong(loaded.map(CheckpointStore.Checkpoint::scanned).orElse(0L));
        AtomicLong encrypted = new AtomicLong(loaded.map(CheckpointStore.Checkpoint::encrypted).orElse(0L));
        AtomicLong written = new AtomicLong(loaded.map(CheckpointStore.Checkpoint::written).orElse(0L));
        AtomicLong quarantined = new AtomicLong(loaded.map(CheckpointStore.Checkpoint::quarantined).orElse(0L));
        AtomicLong completedSinceCheckpoint = new AtomicLong(0L);

        GetOptions getOptions = GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE);
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions()
                .transcoder(RawJsonTranscoder.INSTANCE)
                .durability(config.durabilityLevel());

        ScanType scanType = scanTypeForResume(resumeAfterDocId.get());
        ScanOptions scanOptions = ScanOptions.scanOptions().idsOnly(true);
        AtomicBoolean stopRequested = new AtomicBoolean(false);
        AtomicReference<String> lastScanId = new AtomicReference<>(null);
        AtomicBoolean resumeBlockedByFailure = new AtomicBoolean(false);

        Flux<String> ids = source.scan(scanType, scanOptions)
                .map(ScanResult::id)
                .index()
                .handle((Tuple2<Long, String> t, reactor.core.publisher.SynchronousSink<String> sink) -> {
                    long idx = t.getT1();
                    String docId = t.getT2();
                    if (stopRequested.get()) {
                        sink.complete();
                        return;
                    }
                    String prev = lastScanId.getAndSet(docId);
                    if (prev != null && compareKeyUtf8Unsigned(docId, prev) < 0) {
                        sink.error(new IllegalStateException(
                                "RangeScan returned non-monotonic IDs; cannot safely resume using lastSuccessfulDocId (prev="
                                        + prev + ", current=" + docId + ")"));
                        return;
                    }
                    if (idx > 0 && idx % 1000 == 0 && killSwitch.engaged()) {
                        stopRequested.set(true);
                        log.warn("Kill switch engaged; stopping scan after {} ids", idx);
                        sink.complete();
                        return;
                    }
                    sink.next(docId);
                });

        Flux<DocOutcome> outcomes = ids
                .flatMapSequential(
                        docId -> processOne(docId, getOptions, upsertOptions, scanned, encrypted, written)
                                .onErrorResume(e -> {
                                    quarantined.incrementAndGet();
                                    quarantineWriter.write(docId, stageFrom(e), e);
                                    return Mono.just(DocOutcome.quarantined(docId));
                                }),
                        config.maxInFlight());

        return outcomes.concatMap(outcome -> {
                    if (outcome.kind() == OutcomeKind.QUARANTINED) {
                        resumeBlockedByFailure.set(true);
                    }
                    if (outcome.wroteToDestination() && !resumeBlockedByFailure.get()) {
                        resumeAfterDocId.set(outcome.docId());
                    }

                    long completed = completedSinceCheckpoint.incrementAndGet();
                    if (config.checkpointEvery() > 0 && completed % config.checkpointEvery() == 0) {
                        CheckpointStore.Checkpoint checkpoint = snapshot(resumeAfterDocId.get(), scanned, encrypted, written, quarantined);
                        return Mono.fromRunnable(() -> checkpointStore.save(checkpoint))
                                .subscribeOn(Schedulers.boundedElastic())
                                .thenReturn(outcome);
                    }
                    return Mono.just(outcome);
                })
                .then(Mono.fromCallable(() -> {
                    CheckpointStore.Checkpoint checkpoint = snapshot(resumeAfterDocId.get(), scanned, encrypted, written, quarantined);
                    checkpointStore.save(checkpoint);
                    return checkpoint;
                }).subscribeOn(Schedulers.boundedElastic()));
    }

    private Mono<DocOutcome> processOne(
            String docId,
            GetOptions getOptions,
            UpsertOptions upsertOptions,
            AtomicLong scanned,
            AtomicLong encrypted,
            AtomicLong written) {
        return Mono.defer(() -> {
            scanned.incrementAndGet();
            return source.get(docId, getOptions)
                    .onErrorMap(e -> new StageException("GET", e))
                    .map(getResult -> getResult.contentAsBytes())
                    .flatMap(bytes -> Mono.fromRunnable(rateLimiter::acquire)
                            .subscribeOn(Schedulers.boundedElastic())
                            .onErrorMap(e -> new StageException("RATE_LIMIT", e))
                            .thenReturn(bytes))
                    .map(bytes -> {
                        try {
                            byte[] out = piiEncryptor.encrypt(bytes, docId);
                            if (out != bytes) {
                                encrypted.incrementAndGet();
                            }
                            return out;
                        } catch (RuntimeException e) {
                            throw new StageException("ENCRYPT", e);
                        }
                    })
                    .flatMap(payload -> {
                        if (config.dryRun()) {
                            return Mono.just(DocOutcome.dryRun(docId));
                        }
                        return destination.upsert(docId, payload, upsertOptions)
                                .onErrorMap(e -> new StageException("UPSERT", e))
                                .doOnSuccess(ignored -> written.incrementAndGet())
                                .thenReturn(DocOutcome.written(docId));
                    });
        });
    }

    private static ScanType scanTypeForResume(String lastSuccessfulDocId) {
        if (lastSuccessfulDocId == null || lastSuccessfulDocId.isBlank()) {
            return ScanType.rangeScan(null, null);
        }
        return ScanType.rangeScan(ScanTerm.exclusive(lastSuccessfulDocId), null);
    }

    private static CheckpointStore.Checkpoint snapshot(
            String lastSuccessId,
            AtomicLong scanned,
            AtomicLong encrypted,
            AtomicLong written,
            AtomicLong quarantined) {
        return new CheckpointStore.Checkpoint(
                lastSuccessId,
                scanned.get(),
                encrypted.get(),
                written.get(),
                quarantined.get());
    }

    private static String stageFrom(Throwable t) {
        if (t instanceof StageException se) {
            return se.stage();
        }
        return "UNKNOWN";
    }

    private static int compareKeyUtf8Unsigned(String a, String b) {
        byte[] ab = a.getBytes(StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(StandardCharsets.UTF_8);
        int len = Math.min(ab.length, bb.length);
        for (int i = 0; i < len; i++) {
            int ai = ab[i] & 0xFF;
            int bi = bb[i] & 0xFF;
            if (ai != bi) {
                return ai - bi;
            }
        }
        return ab.length - bb.length;
    }

    public record Config(
            int maxInFlight,
            int checkpointEvery,
            boolean dryRun,
            DurabilityLevel durabilityLevel) {
        public Config {
            if (maxInFlight <= 0) {
                throw new IllegalArgumentException("maxInFlight must be > 0");
            }
            if (checkpointEvery < 0) {
                throw new IllegalArgumentException("checkpointEvery must be >= 0");
            }
            durabilityLevel = Objects.requireNonNull(durabilityLevel, "durabilityLevel");
        }
    }

    private enum OutcomeKind {
        WRITTEN,
        DRY_RUN,
        QUARANTINED
    }

    private record DocOutcome(String docId, OutcomeKind kind) {
        static DocOutcome written(String docId) {
            return new DocOutcome(docId, OutcomeKind.WRITTEN);
        }

        static DocOutcome dryRun(String docId) {
            return new DocOutcome(docId, OutcomeKind.DRY_RUN);
        }

        static DocOutcome quarantined(String docId) {
            return new DocOutcome(docId, OutcomeKind.QUARANTINED);
        }

        boolean wroteToDestination() {
            return kind == OutcomeKind.WRITTEN;
        }
    }

    public static final class StageException extends RuntimeException {
        private final String stage;

        public StageException(String stage, Throwable cause) {
            super(cause);
            this.stage = Objects.requireNonNull(stage, "stage");
        }

        public String stage() {
            return stage;
        }
    }
}
