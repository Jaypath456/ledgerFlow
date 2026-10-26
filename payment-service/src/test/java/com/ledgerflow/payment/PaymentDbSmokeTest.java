package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.beans.factory.annotation.Autowired;

class PaymentDbSmokeTest extends PaymentTestSupport {
    @Autowired
    MongoTemplate mongo;

    @Test
    void collectionsAndUniqueIndexesCreatedInOwnDatabase() {
        assertThat(mongo.getDb().getName()).isEqualTo("payments");
        assertThat(mongo.getCollectionNames()).contains("payments", "idempotency_keys", "outbox", "processed_events");
        assertThat(mongo.indexOps("idempotency_keys").getIndexInfo()).anySatisfy(index -> {
            assertThat(index.isUnique()).isTrue();
            assertThat(index.getIndexFields()).anySatisfy(field -> assertThat(field.getKey()).isEqualTo("key"));
        });
    }

    @Test
    void cannotTouchOtherServiceDatabase() {
        assertThatThrownBy(() -> mongo.getMongoDatabaseFactory().getMongoDatabase("ledger")
                .getCollection("anything").find().first())
                .hasMessageContaining("not authorized");
    }
}
