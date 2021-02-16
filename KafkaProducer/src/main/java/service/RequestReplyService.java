package service;

import domain.Numbers;
import domain.NumbersSumResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

@Service
public class RequestReplyService {

    @Value("${kafka.topic.numbers}")
    private String numbersTopic;

    @Value("${kafka.topic.numbersSum}")
    private String numbersSumResultTopic;

    @Autowired
    private ReplyingKafkaTemplate<String, Numbers, NumbersSumResult> replyKafkaTemplate;

    public RequestReplyFuture<String, Numbers, NumbersSumResult> send(Numbers request) {
        ProducerRecord<String, Numbers> record = new ProducerRecord<>(numbersTopic, request);
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, numbersSumResultTopic.getBytes()));

        RequestReplyFuture<String, Numbers, NumbersSumResult> sendAndReceive = replyKafkaTemplate.sendAndReceive(record);
        return sendAndReceive;
    }
}
