package rest;

import domain.Numbers;
import domain.NumbersSumResult;
import domain.Product;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import service.ProducerService;
import service.RequestReplyService;

import java.util.concurrent.ExecutionException;

@RestController
public class RestWS {

    @Autowired
    private ProducerService producerService;
    @Autowired
    private RequestReplyService requestReplyService;

    @PostMapping(value = "/product")
    public ResponseEntity<Void> send(@RequestBody Product product){
        producerService.send(product);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping(value = "/insert")
    public ResponseEntity<Void> insert(){
        for(int i = 1; i<=5; i++){
            producerService.send(new Product("TV",i));
            producerService.send(new Product("AC",i));
            producerService.send(new Product("MOBILE",i));
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @ResponseBody
    @PostMapping(value = "/getSum")
    public NumbersSumResult getSumRequestReply(@RequestBody Numbers numbers) throws InterruptedException, ExecutionException {

        RequestReplyFuture<String, Numbers, NumbersSumResult> requestReplyFuture = this.requestReplyService.send(numbers);

        // confirm if producer produced successfully and print kafka received information
        SendResult<String, Numbers> sendResult = requestReplyFuture.getSendFuture().get();
        sendResult.getProducerRecord()
                  .headers()
                  .forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));

        // Make a call to get the consumer result record and wait for result
        ConsumerRecord<String, NumbersSumResult> consumerRecord = requestReplyFuture.get();
        return consumerRecord.value();
    }

}
