package io.github.vcvitaly.demokafkaconsumer;

import io.github.vcvitaly.producercommon.TestDto;
import io.github.vcvitaly.producercommon.TestType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@Slf4j
public class TestConsumer {

    private final Map<Integer, String> db = new ConcurrentHashMap<>();
    private final LongAdder adderCreated = new LongAdder();
    private final LongAdder adderUpdated = new LongAdder();
    private final Random rand = new Random();

    public TestConsumer() {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this::printStats, 0, 10, TimeUnit.SECONDS);
    }

    @KafkaListener(id = "${kafka.consumer.containerName}", topics = "${kafka.consumer.topic}", autoStartup = "false")
    public void listen(String msg) {
        try {
            final TestDto testDto = JsonUtil.toObject(msg, TestDto.class);
            if (Objects.requireNonNull(testDto.type()) == TestType.CREATE) {
                runVoid(() -> create(testDto.id(), testDto.data()));
            } else if (testDto.type() == TestType.UPDATE) {
                runAsync(() -> update(testDto.id(), testDto.data()));
            }
        } catch (Exception e) {
            log.error("Error: ", e);
        }/* finally {
            ack.acknowledge();
        }*/
    }

    private CompletableFuture<Void> runAsync(Runnable r) {
        return CompletableFuture.runAsync(r)
                .whenComplete((res, e) -> {
                    if (e != null) {
                        log.error("Error while running: ", e);
                        throw new RuntimeException(e);
                    }
                });
    }

    private void runVoid(Runnable r) {
        r.run();
    }

    private void create(Integer id, String data) {
        /*db.put(id, data);
        sleep(rand);
        adderCreated.increment();*/
//        throw new SomeTestException("Oops");
        log.info("[id=%d, data=%s]".formatted(id, data));
    }

    private void update(Integer id, String data) {
        if (db.containsKey(id)) {
            db.put(id, data);
        } else {
            log.error("Trying to update non-existent entry with id : %d".formatted(id));
        }
        sleep(rand);
        adderUpdated.increment();
    }

    private void sleep(Random r) {
        try {
            Thread.sleep(r.nextInt(5));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void printStats() {
        long created = adderCreated.sumThenReset();
        long updated = adderUpdated.sumThenReset();
        if (created > 0 || updated > 0) {
            log.info("Consumed [created=%d,updated=%d] messages".formatted(created, updated));
        }
    }
}
