package com.ledgerflow.common;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Only the chaos profile creates this bean, so normal runs have no injected faults. */
public class FaultInjector {

    public enum Point { AFTER_LEDGER_COMMIT_BEFORE_ACK, AFTER_OUTBOX_PUBLISH_BEFORE_MARK, BEFORE_PAYMENT_OUTBOX_COMMIT }

    public static final class InjectedFault extends RuntimeException {
        InjectedFault(Point point) {
            super("injected fault at " + point);
        }
    }

    private static final Log log = LogFactory.getLog(FaultInjector.class);

    private final Set<Point> points;
    private final double probability;
    private final boolean halt;
    private final Set<Point> armedOnce = ConcurrentHashMap.newKeySet();

    public FaultInjector(Set<Point> points, double probability, boolean halt) {
        this.points = points;
        this.probability = probability;
        this.halt = halt;
        log.warn("chaos profile active: points=" + points + " probability=" + probability + " halt=" + halt);
    }

    public void hit(Point point) {
        if (armedOnce.remove(point)) {
            throw new InjectedFault(point);
        }
        if (points.contains(point) && ThreadLocalRandom.current().nextDouble() < probability) {
            if (halt) {
                log.error("CHAOS: halting JVM at " + point);
                Runtime.getRuntime().halt(1);
            }
            throw new InjectedFault(point);
        }
    }

    /** Tests can make the next hit throw once. */
    public void armOnce(Point point) {
        armedOnce.add(point);
    }
}
