package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

class LedgerDbSmokeTest {
    private final MongoTemplate mongo = MongoTestSupport.mongo();

    @Test
    void collectionsAndUniqueIndexesCreatedInOwnDatabase() {
        assertThat(mongo.getDb().getName()).isEqualTo("ledger");
        assertThat(mongo.getCollectionNames()).contains("accounts", "ledger_transactions", "ledger_entries", "payment_outcomes", "outbox", "processed_events");
        assertThat(mongo.indexOps("ledger_transactions").getIndexInfo()).anySatisfy(index -> {
            assertThat(index.isUnique()).isTrue();
            assertThat(index.getIndexFields()).anySatisfy(field -> assertThat(field.getKey()).isEqualTo("payment_id"));
        });
    }

    @Test
    void cannotTouchOtherServiceDatabase() {
        assertThatThrownBy(() -> mongo.getMongoDatabaseFactory().getMongoDatabase("payments")
                .getCollection("anything").find().first())
                .hasMessageContaining("not authorized");
    }
}
