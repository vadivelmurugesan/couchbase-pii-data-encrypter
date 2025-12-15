package com.example.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CouchbaseClients implements AutoCloseable {
    private final ClusterEnvironment environment;
    private final Cluster sourceCluster;
    private final Cluster destinationCluster;
    private final Collection sourceCollection;
    private final Collection destinationCollection;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Duration shutdownTimeout;

    private CouchbaseClients(
            ClusterEnvironment environment,
            Cluster sourceCluster,
            Cluster destinationCluster,
            Collection sourceCollection,
            Collection destinationCollection,
            Duration shutdownTimeout) {
        this.environment = Objects.requireNonNull(environment, "environment");
        this.sourceCluster = Objects.requireNonNull(sourceCluster, "sourceCluster");
        this.destinationCluster = Objects.requireNonNull(destinationCluster, "destinationCluster");
        this.sourceCollection = Objects.requireNonNull(sourceCollection, "sourceCollection");
        this.destinationCollection = Objects.requireNonNull(destinationCollection, "destinationCollection");
        this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
    }

    public static CouchbaseClients connect(EnvironmentConfig env, ClusterConfig source, ClusterConfig destination) {
        Objects.requireNonNull(env, "env");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");

        ClusterEnvironment environment = ClusterEnvironment.builder()
                .timeoutConfig(tc -> tc
                        .kvTimeout(env.kvTimeout())
                        .connectTimeout(env.connectTimeout()))
                .ioConfig(io -> io.numKvConnections(env.numKvConnections()))
                .build();

        Cluster sourceCluster = null;
        Cluster destinationCluster = null;
        boolean ok = false;
        try {
            sourceCluster = connectCluster(source, environment);
            destinationCluster = connectCluster(destination, environment);

            Collection sourceCollection = openCollection(sourceCluster, source, env.waitUntilReadyTimeout());
            Collection destinationCollection = openCollection(destinationCluster, destination, env.waitUntilReadyTimeout());

            CouchbaseClients clients = new CouchbaseClients(
                    environment,
                    sourceCluster,
                    destinationCluster,
                    sourceCollection,
                    destinationCollection,
                    env.shutdownTimeout());
            ok = true;
            return clients;
        } finally {
            if (!ok) {
                safeDisconnect(destinationCluster);
                safeDisconnect(sourceCluster);
                safeShutdown(environment, env.shutdownTimeout());
            }
        }
    }

    public Collection sourceCollection() {
        return sourceCollection;
    }

    public Collection destinationCollection() {
        return destinationCollection;
    }

    public ReactiveCollection sourceReactiveCollection() {
        return sourceCollection.reactive();
    }

    public ReactiveCollection destinationReactiveCollection() {
        return destinationCollection.reactive();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        RuntimeException first = null;
        try {
            destinationCluster.disconnect();
        } catch (RuntimeException e) {
            first = e;
        }
        try {
            sourceCluster.disconnect();
        } catch (RuntimeException e) {
            if (first == null) {
                first = e;
            }
        }
        try {
            environment.shutdown(shutdownTimeout);
        } catch (RuntimeException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private static Cluster connectCluster(ClusterConfig config, ClusterEnvironment environment) {
        ClusterOptions options = ClusterOptions.clusterOptions(config.username(), config.password())
                .environment(environment);
        return Cluster.connect(config.connectionString(), options);
    }

    private static Collection openCollection(Cluster cluster, ClusterConfig config, Duration waitUntilReadyTimeout) {
        Bucket bucket = cluster.bucket(config.bucketName());
        bucket.waitUntilReady(waitUntilReadyTimeout);
        Scope scope = bucket.scope(config.scopeName());
        return scope.collection(config.collectionName());
    }

    private static void safeDisconnect(Cluster cluster) {
        if (cluster == null) {
            return;
        }
        try {
            cluster.disconnect();
        } catch (RuntimeException ignored) {
        }
    }

    private static void safeShutdown(ClusterEnvironment environment, Duration shutdownTimeout) {
        if (environment == null) {
            return;
        }
        try {
            environment.shutdown(shutdownTimeout);
        } catch (RuntimeException ignored) {
        }
    }

    public record EnvironmentConfig(
            Duration kvTimeout,
            Duration connectTimeout,
            int numKvConnections,
            Duration waitUntilReadyTimeout,
            Duration shutdownTimeout) {
        public EnvironmentConfig {
            Objects.requireNonNull(kvTimeout, "kvTimeout");
            Objects.requireNonNull(connectTimeout, "connectTimeout");
            Objects.requireNonNull(waitUntilReadyTimeout, "waitUntilReadyTimeout");
            Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
            if (kvTimeout.isNegative() || kvTimeout.isZero()) {
                throw new IllegalArgumentException("kvTimeout must be > 0");
            }
            if (connectTimeout.isNegative() || connectTimeout.isZero()) {
                throw new IllegalArgumentException("connectTimeout must be > 0");
            }
            if (numKvConnections <= 0) {
                throw new IllegalArgumentException("numKvConnections must be > 0");
            }
        }

        public static EnvironmentConfig defaults() {
            return new EnvironmentConfig(
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(10),
                    2,
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(10));
        }
    }

    public record ClusterConfig(
            String connectionString,
            String username,
            String password,
            String bucketName,
            String scopeName,
            String collectionName) {
        public ClusterConfig {
            connectionString = requireNotBlank(connectionString, "connectionString");
            username = requireNotBlank(username, "username");
            password = requireNotBlank(password, "password");
            bucketName = requireNotBlank(bucketName, "bucketName");
            scopeName = requireNotBlank(scopeName, "scopeName");
            collectionName = requireNotBlank(collectionName, "collectionName");
        }
    }

    private static String requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
