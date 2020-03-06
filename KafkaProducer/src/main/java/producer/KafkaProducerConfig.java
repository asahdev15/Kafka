package producer;

import asahdev.models.NumbersModel;
import asahdev.models.ResultSumModel;
import asahdev.models.User;
import asahdev.models.Product;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.reply}")
    private String replyTopic;

    @Value("${kafka.consumer.group-id}")
    private String requestReplyConsumerGroupId;

    @Value("${kafka.topic.product}")
    private String topicProduct;

    @Value("${kafka.topic.user}")
    private String topicUser;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, requestReplyConsumerGroupId);
        return props;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    // Topic will be created during start up
    @Bean
    public NewTopic createTopicProduct() {
        return new NewTopic(topicProduct, 1, (short) 1);
    }

    @Bean
    public NewTopic createTopicUser() {
        return new NewTopic(topicUser, 1, (short) 1);
    }

    @Bean
    public KafkaTemplate<String, User> kafkaTemplateUser() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
    }

    @Bean
    public KafkaTemplate<String, Product> kafkaTemplateProduct() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()));
    }

    @Bean
    public ReplyingKafkaTemplate<String, NumbersModel, ResultSumModel> replyKafkaTemplate() {
        return new ReplyingKafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()), replyListenerContainer());
    }

    @Bean
    public KafkaMessageListenerContainer<String, ResultSumModel> replyListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        return new KafkaMessageListenerContainer<>(replyConsumerFactory(), containerProperties);
    }

    @Bean
    public ConsumerFactory<String, ResultSumModel> replyConsumerFactory() {
        JsonDeserializer<ResultSumModel> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages(ResultSumModel.class.getPackage().getName());
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), jsonDeserializer);
    }

}