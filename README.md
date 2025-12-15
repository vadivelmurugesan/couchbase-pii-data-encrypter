# cb-pii-migrate

KV-only Couchbase migration tool that scans documents via RangeScan, detects PII by key name, and performs field-level encryption using AES-256-GCM with AAD bound to the document ID.

It runs a simple pipeline (`RangeScan → GET → encrypt → UPSERT`) using the Couchbase Java SDK reactive KV APIs (no N1QL). Operational safety features include a startup safety gate, atomic checkpoints for resume, a kill switch, rate limiting, bounded concurrency, and quarantine files for per-document failures (without writing document bodies).

## Build

- Java 17
- Maven 3

Commands:
- `mvn -q test`
- `mvn -q -DskipTests package`

## Required environment variables

Keystore (required; on-prem only):

- `KEYSTORE_PATH` (e.g. `/secure/keys/pii.p12`)
- `KEYSTORE_TYPE` (optional; `PKCS12` default; `JCEKS` supported)
- `KEYSTORE_PASSWORD`
- `KEYSTORE_ALIAS`
- `KEYSTORE_KEY_PASSWORD` (optional; defaults to `KEYSTORE_PASSWORD`)
- `KEY_ID` (optional; defaults to `KEYSTORE_ALIAS`; written to audit file)

Couchbase credentials (recommended via env vars, not the properties file):
- `SOURCE_COUCHBASE_USERNAME`
- `SOURCE_COUCHBASE_PASSWORD`
- `DESTINATION_COUCHBASE_USERNAME`
- `DESTINATION_COUCHBASE_PASSWORD`

## Configuration

By default the app reads `application.properties` from the working directory. Override with:
- `-Dapp.properties=/path/to/application.properties`

`application.properties` configures:
- Source/destination connection strings and collection targets
- Environment tuning (`couchbase.kvTimeout`, `couchbase.connectTimeout`, `couchbase.numKvConnections`, etc.)
- PII detection rules (`pii.keys` and/or `pii.keyRegex`) — at least one is required
- Operational controls (dry-run, durability, checkpoint/quarantine/audit paths, kill switch)

Safety gate (required):
- `sourceWritesFrozen=true` or startup aborts.

## Run (Dry-Run)

Dry-run still performs `RangeScan → GET → encrypt`, but skips `UPSERT`.

1) Set `migration.dryRun=true` in `application.properties`
2) Run:
- `mvn -q -DskipTests package`
- `mvn -q -DskipTests dependency:build-classpath -Dmdep.outputFile=target/cp.txt`
- `java -cp "$(cat target/cp.txt):target/cb-pii-migrate-0.1.0-SNAPSHOT.jar" -Dapp.properties=application.properties com.example.App`

## Run (Production)

1) Set:
- `sourceWritesFrozen=true`
- `migration.dryRun=false`
- `migration.durability=MAJORITY` (or your required durability level)
2) Run with pinned CPU/RAM:
- `mvn -q -DskipTests package`
- `mvn -q -DskipTests dependency:build-classpath -Dmdep.outputFile=target/cp.txt`
- `java -XX:ActiveProcessorCount=2 -Xms16g -Xmx16g -cp "$(cat target/cp.txt):target/cb-pii-migrate-0.1.0-SNAPSHOT.jar" -Dapp.properties=application.properties com.example.App`

## Kill switch

Configured via:
- `migration.killSwitch.enabled` (boolean)
- `migration.killSwitch.path` (file path)

To stop the run, create the kill switch file while the job is running:
- `touch kill.switch`

The job checks the kill switch every 1,000 scanned document IDs and stops after the next check.

## Resume behavior

The job persists `lastSuccessfulDocId` in the checkpoint and resumes the RangeScan from the exclusive next ID after that value.

Important:
- The resume cursor advances only on successful destination writes.
- If any document is quarantined, the resume cursor stops advancing for the rest of the run (to avoid skipping failures). A restart will re-scan from the last successful write prior to the first quarantine and may reprocess already-migrated documents (they should remain unchanged because already-encrypted fields are skipped).
- Document IDs are compared using unsigned byte-wise UTF-8 ordering for a monotonicity safety check. If RangeScan returns non-monotonic IDs, the job aborts rather than writing an unsafe resume checkpoint. Prefer stable ASCII/UTF-8 document IDs for predictable ordering.

## Output locations

Configured via `application.properties`:
- Checkpoint file: `migration.checkpoint.path` (default `checkpoints/checkpoint.dat`)
- Quarantine directory: `migration.quarantine.path` (default `quarantine/`)
  - Contains `docId`, `stage`, and exception metadata only (never document content)
- Audit directory: `migration.audit.dir` (default `audit/`)
  - Writes `audit-<runId>.json` with timestamps, config checksum, key id, durability, dry-run flag, and final counts
