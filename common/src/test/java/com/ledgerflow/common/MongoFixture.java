package com.ledgerflow.common;

import org.testcontainers.mongodb.MongoDBContainer;

/** Authenticated single-node replica set shared by each module's real-service tests. */
public final class MongoFixture {
    private MongoFixture() {}

    public static MongoDBContainer start() {
        var mongo = new MongoDBContainer("mongo:8.0").withReplicaSet()
                .withCommand("bash", "-c", "printf '%s' ledgerflowTestReplicaSetKey > /tmp/ledgerflow-key; "
                        + "chmod 400 /tmp/ledgerflow-key; exec mongod --replSet rs0 --bind_ip_all --keyFile /tmp/ledgerflow-key");
        mongo.start();
        try {
            var result = mongo.execInContainer("mongosh", "admin", "--quiet", "--eval", """
                    db.createUser({user:'test_admin',pwd:'test_admin',roles:['root']});
                    db.auth('test_admin','test_admin');
                    db.getSiblingDB('payments').createUser({user:'payment_user',pwd:'payment_user',roles:[{role:'readWrite',db:'payments'}]});
                    db.getSiblingDB('ledger').createUser({user:'ledger_user',pwd:'ledger_user',roles:[{role:'readWrite',db:'ledger'}]});
                    """);
            if (result.getExitCode() != 0) throw new IllegalStateException(result.getStdout() + result.getStderr());
        } catch (Exception e) {
            mongo.stop();
            throw new IllegalStateException("MongoDB test users could not be created", e);
        }
        return mongo;
    }

    public static String uri(MongoDBContainer mongo, String database) {
        String user = switch (database) {
            case "payments" -> "payment_user";
            case "ledger" -> "ledger_user";
            case "admin" -> "test_admin";
            default -> throw new IllegalArgumentException(database);
        };
        return "mongodb://" + user + ":" + user + "@" + mongo.getHost() + ":" + mongo.getMappedPort(27017)
                + "/" + database + "?authSource=" + database + "&replicaSet=rs0&directConnection=true";
    }
}
