package consumer;

import com.asahdev.common.models.NumbersModel;
import com.asahdev.common.models.ResultSumModel;
import com.asahdev.common.models.User;
import lombok.extern.slf4j.Slf4j;
import models.Product;
import models.ProductV2;
import models.ProductV3;
import models.ProductV4;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;
import utils.JsonUtils;
import utils.RestUtils;

import java.util.List;

@Slf4j
@Service
public class ConsumerService {

    @KafkaListener( topics = "${kafka.topic.test}",
                    containerFactory = "basicCF")
    public void batchCF(@Payload List<String> messages,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                        @Header(KafkaHeaders.OFFSET) List<Long> offsets,
                        Acknowledgment acknowledgment) {
        log.info("---- START ----");
        for (int i = 0; i < messages.size(); i++) {
            log.info("Received | Topic: {} | Partition: {} | Offset: {} | Message:{}",
                    topics.get(i), partitions.get(i), offsets.get(i), messages.get(i));
        }
        acknowledgment.acknowledge();
        log.info("---- END ----");
    }

    @KafkaListener( topics = "${kafka.topic.request}",
                    containerFactory = "requestReplyCF")
    @SendTo()
    public ResultSumModel requestReplyCF(NumbersModel numbersModel) {
        log.info("Received Message : " + numbersModel);
        ResultSumModel resultSumModel = new ResultSumModel(numbersModel.getFirstNumber() + numbersModel.getSecondNumber());
        log.info("Sending result : " + resultSumModel);
        return resultSumModel;
    }

    // Listen to topic-partition-offset - applicable to topic with multiple partitions
//    Since the initialOffset has been sent to 0 in this listener,
//    all the previously consumed messages from partitions 0 and three will be re-consumed
//    every time this listener is initialized
//    @KafkaListener(
//            topicPartitions = @TopicPartition(topic = "topicName",
//                                            partitionOffsets = {
//                                                    @PartitionOffset(partition = "0", initialOffset = "0"),
//                                                    @PartitionOffset(partition = "3", initialOffset = "0")
//                    }))
    // Listening to provided topic multiple partition
//    @KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topic.user}",
//                                                     partitions = { "0", "1"}),
//                   containerFactory = "consumeUser")
    @KafkaListener( topics = "${kafka.topic.user}",
                    containerFactory = "userCF")
    public void user(ConsumerRecord<String, User> payload)
    {
        log.info("---- START ----");
        log.info("Consume | Payload | " + payload.value());
        RestUtils.post("http://localhost:8080/api/v1/user", User.class, payload.value());
        log.info("---- END ----");
    }

    @KafkaListener( topics = "${kafka.topic.product}",
                    containerFactory = "productCF")
    public void product(ConsumerRecord<String, Product> payload)
    {
        Product product = payload.value();
        String recieved = "";
        if(product instanceof ProductV4){
            recieved = "V4 recieved";
        }else if(product instanceof ProductV3){
            recieved = "V3 recieved";
        }else if(product instanceof ProductV2){
            recieved = "V2 recieved";
        }else {
            recieved = "V1 recieved";
        }
        log.info(recieved + " : " + product + ", Payload : " + JsonUtils.convertToJson(product));
    }

}
