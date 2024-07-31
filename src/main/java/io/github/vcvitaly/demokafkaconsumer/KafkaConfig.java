package io.github.vcvitaly.demokafkaconsumer;

import io.github.vcvitaly.producercommon.TestDto;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.id}")
    private String testGroup;

    public Map<String, Object> consumerConfigsCommon() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public Map<String, Object> consumerConfigsForTest() {
        Map<String, Object> props = new HashMap<>(consumerConfigsCommon());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, testGroup);
        return props;
    }

    @Bean(name = "testFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TestDto>> kafkaListenerContainerFactoryForTest() {
        ConcurrentKafkaListenerContainerFactory<String, TestDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(consumerConfigsForTest()));
        factory.setCommonErrorHandler(errorHandler());
        factory.setAutoStartup(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private DefaultErrorHandler errorHandler() {
        var backOff = new ExponentialBackOffWithMaxRetries(7);
        backOff.setInitialInterval(1_000);
        backOff.setMaxInterval(10_000);
        var errorHandler = new DefaultErrorHandler(backOff);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    private ConsumerFactory<String, TestDto> consumerFactory(Map<String, Object> consumerConfig) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig,
                new StringDeserializer(),
                makeJsonDeserializer(TestDto.class)
        );
    }

    private static <T> Deserializer<T> makeJsonDeserializer(Class<? super T> targetType) {
        JsonDeserializer<T> tsDeserializer = new JsonDeserializer<>(targetType);
        tsDeserializer.addTrustedPackages("*");
        tsDeserializer.setUseTypeMapperForKey(false);
        tsDeserializer.setUseTypeHeaders(false);
        return new ErrorHandlingDeserializer<>(tsDeserializer);
    }
}
