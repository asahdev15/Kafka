package config;

import domain.Numbers;
import domain.NumbersSumResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class RequestReplyConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.numbersSum}")
    private String numbersSumTopic;

    @Value("${kafka.consumer.group-id}")
    private String consumerGroupId;

    @Bean
    public Map<String, Object> producerConfigs2() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> consumerConfigs2() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        return props;
    }

    @Bean
    public ReplyingKafkaTemplate<String, Numbers, NumbersSumResult> replyKafkaTemplate() {

        // consumer settings
        JsonDeserializer<NumbersSumResult> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages(NumbersSumResult.class.getPackage().getName());
        ConsumerFactory<String, NumbersSumResult> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerConfigs2(),
                                                  new StringDeserializer(),
                                                  jsonDeserializer);

        KafkaMessageListenerContainer<String, NumbersSumResult> kafkaMessageListenerContainer =
                new KafkaMessageListenerContainer<>(consumerFactory, new ContainerProperties(numbersSumTopic));


        return new ReplyingKafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerConfigs2()),
                kafkaMessageListenerContainer);
    }

}
