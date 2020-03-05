package producer;

import com.asahdev.common.models.NumbersModel;
import com.asahdev.common.models.ResultSumModel;
import com.asahdev.common.models.User;
import models.Product;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class KafkaProducerService {

    @Value("${kafka.topic.request}")
    private String requestTopic;

    @Value("${kafka.topic.reply}")
    private String requestReplyTopic;

    @Value("${kafka.topic.user}")
    private String topicUser;

    @Value("${kafka.topic.product}")
    private String topicProduct;

    @Autowired
    private ReplyingKafkaTemplate<String, NumbersModel, ResultSumModel> requestReplyKafkaTemplate;

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplateUser;

    @Autowired
    private KafkaTemplate<String, Product> kafkaTemplateProduct;

    public void sendUser(User user) {
        ListenableFuture<SendResult<String, User>> future = kafkaTemplateUser.send(topicUser, user);
        future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
            @Override
            public void onSuccess(SendResult<String, User> result) {
                StringBuilder sb = new StringBuilder("Kafka | Sent: ");
                sb.append("Message=[").append(user).append("];");
                sb.append("Record Metadata=[").append(result.getRecordMetadata().toString()).append("];");
                System.out.println(sb.toString());
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Kafka | Unable to send message=[" + user + "] due to : " + ex.getMessage());
            }
        });
    }

    public void sendProduct(Product data) {
        System.out.println(String.format("Kafka pushing product -> %s", data));
        ListenableFuture<SendResult<String, Product>> future = kafkaTemplateProduct.send(topicProduct, data);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Product>>() {
            @Override
            public void onSuccess(SendResult<String, Product> result) {
                StringBuilder sb = new StringBuilder("Sent: ");
                sb.append("Message=[").append(data).append("];");
                sb.append("Record Metadata=[").append(result.getRecordMetadata().toString()).append("];");
                System.out.println(sb.toString());
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=[" + data + "] due to : " + ex.getMessage());
            }
        });
    }

    public RequestReplyFuture<String, NumbersModel, ResultSumModel> sendNumbersModelRequest(NumbersModel request) {
        ProducerRecord<String, NumbersModel> record = new ProducerRecord<String, NumbersModel>(requestTopic, request);
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
        RequestReplyFuture<String, NumbersModel, ResultSumModel> sendAndReceive = requestReplyKafkaTemplate.sendAndReceive(record);
        return sendAndReceive;
    }

}
