package service;

import domain.Product;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducerService {

    @Value("${kafka.topic.product}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, Product> kafkaTemplate;

    public void send(Product product) {
        ListenableFuture<SendResult<String, Product>> future = kafkaTemplate.send(topic, product.getCategory(), product);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Product>>() {
            @Override
            public void onSuccess(SendResult<String, Product> result) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                StringBuilder sb = new StringBuilder();
                sb.append("; Topic:"+ recordMetadata.topic());
                sb.append("; PartitionKey:"+ result.getProducerRecord().key());
                sb.append("; Partition:"+ recordMetadata.partition());
                sb.append("; Offset:"+ recordMetadata.offset());
                System.out.println("Success: " + sb.toString());
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Failed: " + ex.getMessage());
            }
        });
    }
}
