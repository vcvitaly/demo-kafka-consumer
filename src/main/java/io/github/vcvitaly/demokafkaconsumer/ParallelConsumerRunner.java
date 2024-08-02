package io.github.vcvitaly.demokafkaconsumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.github.vcvitaly.producercommon.TestDto;
import jakarta.annotation.PostConstruct;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class ParallelConsumerRunner implements ApplicationRunner {

    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<String, String> cf;
    private final String containerName;
    private final String topic;

    public ParallelConsumerRunner(KafkaListenerEndpointRegistry registry,
                                  ConsumerFactory<String, String> cf,
                                  @Value("${kafka.consumer.containerName}") String containerName,
                                  @Value("${kafka.consumer.topic}") String topic) {
        this.registry = registry;
        this.cf = cf;
        this.containerName = containerName;
        this.topic = topic;
    }

    @Override
    public void run(ApplicationArguments args) {
        run();
    }

    private void run() {
        MessageListener messageListener = (MessageListener) registry.getListenerContainer(containerName)
                .getContainerProperties().getMessageListener();
        Consumer<String, String> consumer = cf.createConsumer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(consumer)
                .maxConcurrency(10)
                .build();
        ParallelStreamProcessor<String, String> processor = ParallelStreamProcessor
                .createEosStreamProcessor(options);
        processor.subscribe(List.of(topic));
        processor.poll(context -> messageListener.onMessage(context.getSingleConsumerRecord(), null, consumer));
    }
}
