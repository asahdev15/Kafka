package producer;

import asahdev.models.*;
import asahdev.utils.JsonUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;
import java.util.concurrent.ExecutionException;

@RestController
public class RestControllerWS {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @ResponseBody
    @PostMapping(value = "/sum")
    public ResultSumModel sum(@RequestBody NumbersModel request) throws InterruptedException, ExecutionException {
        ConsumerRecord<String, ResultSumModel> result = sendMessage(request);
        return result.value();
    }

    private ConsumerRecord<String, ResultSumModel> sendMessage(NumbersModel request) throws InterruptedException, ExecutionException {
        RequestReplyFuture<String, NumbersModel, ResultSumModel> sendAndReceive = this.kafkaProducerService.sendNumbersModelRequest(request);
        // confirm if producer produced successfully and print kafka recieved information
        SendResult<String, NumbersModel> sendResult = sendAndReceive.getSendFuture().get();
        sendResult.getProducerRecord().headers().forEach(
                header -> System.out.println(header.key() + ":" + header.value().toString())
        );
        // Make a call to get the consumer result record and wait for result
        ConsumerRecord<String, ResultSumModel> consumerRecord = sendAndReceive.get();
        return consumerRecord;
    }

    @PostMapping(value = "/sendUser")
    public ResponseEntity<Void> sendUser(@RequestBody User user){
        System.out.println("REST | Received Payload : " + JsonUtils.convertToJson(user));
        kafkaProducerService.sendUser(user);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping(value = "/sendProduct")
    public ResponseEntity<ProductDetails> updateProduct(@RequestBody Product product){
        System.out.println("---------------------> Received Product : " + product + ", Payload : " + JsonUtils.convertToJson(product));
        kafkaProducerService.sendProduct(product);
        return new ResponseEntity<>(checkProductVersion(product), HttpStatus.OK);
    }

    private ProductDetails checkProductVersion(Product product){
        ProductDetails productDetails = new ProductDetails();
        if(product instanceof ProductV4){
            productDetails.version = "V4 logic will be invoked";
        }else if(product instanceof ProductV3){
            productDetails.version = "V3 logic will be invoked";
        }else if(product instanceof ProductV2){
            productDetails.version = "V2 logic will be invoked";
        }else {
            productDetails.version = "V1 logic will be invoked";
        }
        return productDetails;
    }

}
