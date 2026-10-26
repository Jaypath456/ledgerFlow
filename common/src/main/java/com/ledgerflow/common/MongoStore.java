package com.ledgerflow.common;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import java.util.Date;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;

/** One session per operation; nested service calls share the driver's retryable transaction. */
public final class MongoStore {
    private final MongoClient client;
    private final MongoTemplate template;
    private final ThreadLocal<MongoTemplate> active = new ThreadLocal<>();
    private static final TransactionOptions OPTIONS = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build();

    public MongoStore(MongoClient client, MongoTemplate template) {
        this.client = client;
        this.template = template;
        collection("outbox");
        collection("processed_events");
        unique("processed_events", "event_id");
        template.indexOps("outbox").createIndex(new Index().on("published_at", Sort.Direction.ASC).on("_id", Sort.Direction.ASC));
    }

    public MongoTemplate mongo() {
        return active.get() == null ? template : active.get();
    }

    public boolean inTransaction() {
        return active.get() != null;
    }

    public <T> T transaction(Supplier<T> work) {
        if (active.get() != null) return work.get();
        try (var session = client.startSession()) {
            return session.withTransaction(() -> {
                active.set(template.withSession(session));
                try {
                    return work.get();
                } catch (DataAccessException e) {
                    // The driver must see the original labels to retry write conflicts.
                    for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                        if (cause instanceof MongoException mongo) throw mongo;
                    }
                    throw e;
                } finally {
                    active.remove();
                }
            }, OPTIONS);
        } catch (MongoException e) {
            var translated = new MongoExceptionTranslator().translateExceptionIfPossible(e);
            if (translated != null) throw translated;
            throw e;
        }
    }

    public void collection(String name) {
        if (!template.collectionExists(name)) template.createCollection(name);
    }

    public void unique(String collection, String field) {
        template.indexOps(collection).createIndex(new Index().on(field, Sort.Direction.ASC).unique());
    }

    public static Query eq(String field, Object value) {
        return Query.query(Criteria.where(field).is(value instanceof UUID id ? id.toString() : value));
    }

    public static long nextId() {
        return UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    }

    public static Document account(long id, String type) {
        return new Document("_id", id).append("type", type).append("balance_minor", 0L);
    }

    public boolean processed(UUID id) {
        return mongo().exists(eq("event_id", id), "processed_events");
    }

    public void recordEvent(UUID id) {
        mongo().insert(new Document("_id", id.toString()).append("event_id", id.toString())
                .append("processed_at", new Date()), "processed_events");
    }
}
