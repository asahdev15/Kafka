package service;

import domain.Product;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    // consume user with default consumer group
//    @KafkaListener(
//            topics = "${kafka.topic.product}",
//            containerFactory = "consumeProduct")
//    public void consume(ConsumerRecord<String, Product> record) {
//        System.out.println(getRecordDetail(record));
//    }


    // consume user with specific consumer group
//    @KafkaListener(
//            topics = "${kafka.topic.user}",
//            groupId = "cg-user-NEW",
//            containerFactory = "consumeUser")
//    public void consumeUser2(ConsumerRecord<String, Product> user) {
//        System.out.println("User NEW : " + user.toString());
//    }

    // consume user with default consumer group and specific topic-partitions
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.product}",partitions = {"0"}),
            containerFactory = "consumeProduct")
    public void consumeUser3(ConsumerRecord<String, Product> record) {
        System.out.println("AC => " + getRecordDetail(record));
    }
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.product}",partitions = {"1"}),
            containerFactory = "consumeProduct")
    public void consumeUser2(ConsumerRecord<String, Product> record) {
        System.out.println("MOBILE=> " + getRecordDetail(record));
    }
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.product}", partitions = {"2"}),
            containerFactory = "consumeProduct")
    public void consumeUser4(ConsumerRecord<String, Product> record) {
        System.out.println("TV => " + getRecordDetail(record));
    }
//
//    // consume user with default consumer group and specific topic-partitions-offset
//    @KafkaListener(
//            topicPartitions = @TopicPartition(
//                    topic = "${kafka.topic.user}",
//                    partitionOffsets = {
//                            @PartitionOffset(partition = "0", initialOffset = "0"),
//                            @PartitionOffset(partition = "2", initialOffset = "0")}),
//            containerFactory = "consumeUser")
//    public void consumeUser4(ConsumerRecord<String, User> payload) {
//        System.out.println("User : " + payload.toString());
//    }


//    // consume users in batch
//    @KafkaListener(
//            topics = "${kafka.topic.user}",
//            containerFactory = "consumeUser")
//    public void consumeUserBatch(
//            @Payload List<User> messages,
//            @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
//            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
//            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
//            Acknowledgment acknowledgment) {
//        for (int i = 0; i < messages.size(); i++) {
//            StringBuilder sb = new StringBuilder();
//            sb.append(";Topic: " + topics.get(i));
//            sb.append(";Partition: " + partitions.get(i));
//            sb.append(";Offset: " + offsets.get(i));
//            sb.append(";Message: " + messages.get(i));
//            System.out.println(sb.toString());
//        }
//        acknowledgment.acknowledge();
//    }





    // Request-Reply pattern

//    @KafkaListener( topics = "${kafka.topic.request}",
//                    containerFactory = "requestReplyCF")
//    @SendTo()
//    public ResultSumModel requestReplyCF(NumbersModel numbersModel) {
//        ResultSumModel resultSumModel = new ResultSumModel(numbersModel.getFirstNumber() + numbersModel.getSecondNumber());
//        return resultSumModel;
//    }


    private static String getRecordDetail(ConsumerRecord<String, Product> record){
        StringBuilder sb = new StringBuilder();
        sb.append("; Topic:"+ record.topic());
        sb.append("; Partition:"+ record.partition());
        sb.append("; Offset:"+ record.offset());
        sb.append("; Record:"+ record.value());
        return sb.toString();
    }


}
