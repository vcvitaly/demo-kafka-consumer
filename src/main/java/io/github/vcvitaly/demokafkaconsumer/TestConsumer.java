package io.github.vcvitaly.demokafkaconsumer;

import io.github.vcvitaly.producercommon.TestDto;
import io.github.vcvitaly.producercommon.TestType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@Slf4j
public class TestConsumer {

    private final Map<Integer, String> db = new ConcurrentHashMap<>();
    private final LongAdder adder = new LongAdder();

    public TestConsumer() {
        Executors.newSingleThreadScheduledExecutor().schedule(this::printStats, 10, TimeUnit.SECONDS);
    }

    @KafkaListener(topics = "${kafka.consumer.topic}")
    public void listen(TestDto testDto) {
        if (Objects.requireNonNull(testDto.type()) == TestType.CREATE) {
            create(testDto.id(), testDto.data());
        } else if (testDto.type() == TestType.UPDATE) {
            update(testDto.id(), testDto.data());
        }
    }

    private void create(Integer id, String data) {
        db.put(id, data);
    }

    private void update(Integer id, String data) {
        if (db.containsKey(id)) {
            db.put(id, data);
        } else {
            log.error("Trying to update non-existent entry with id : %d".formatted(id));
        }
    }

    private void printStats() {
        long l = adder.sumThenReset();
        if (l > 0) {
            log.info("Consumed [%d] messages".formatted(l));
        }
    }
}
