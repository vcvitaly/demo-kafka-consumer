package io.github.vcvitaly.demokafkaconsumer;

import io.github.vcvitaly.producercommon.TestDto;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupId}")
    private String testGroup;

    private Map<String, Object> consumerConfigsCommon() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private Map<String, Object> consumerConfigsForTest() {
        Map<String, Object> props = new HashMap<>(consumerConfigsCommon());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, testGroup);
        return props;
    }

    @Bean(name = "testFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactoryForTest() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryForTest());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactoryForTest() {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfigsForTest(), new StringDeserializer(), new StringDeserializer()
        );
    }

    private static <T> Deserializer<T> makeJsonDeserializer(Class<? super T> targetType) {
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetType);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);
        deserializer.setUseTypeHeaders(false);
        return new ErrorHandlingDeserializer<>(deserializer);
    }
}
