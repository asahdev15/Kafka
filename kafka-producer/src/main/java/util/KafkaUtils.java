package util;

import com.google.common.collect.Sets;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {

    private static String kafkaServer = "kafka-0:9092";

    public static void main(String[] args) throws Exception{
        isKakfaServerRunning();
    }

    private static void isKafkaRunning(){
        long startTime = System.currentTimeMillis();
        boolean check = isKafkaServerValid() && isKakfaServerRunning();
        if(check){
            System.out.println("--------------- Kafka is running");
        }else{
            System.out.println("--------------- Kafka is NOT running");
        }
        System.out.println("Time Taken (secs) : " + (System.currentTimeMillis()-startTime)/1000);
    }

    private static boolean isKakfaServerRunning(){
        if(listTopicNames().isEmpty()){
            System.out.println("Kakfa is not running");
            return false;
        }
        System.out.println("Kakfa is running");
        return true;
    }

    // ------------------------------------------------------------------------------------

    private static void listTopicDescription() throws Exception{
        try (KafkaAdminClient client =  getKafkaAdminClient()){
            Set<String> topics = listTopicNames();
            DescribeTopicsResult result = client.describeTopics(topics);
            Map<String, KafkaFuture<TopicDescription>> values = result.values();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : values.entrySet()) {
                String key = entry.getKey();
                KafkaFuture<TopicDescription> value = entry.getValue();
                System.out.println(value.get());
                //                System.out.println("Topic: " + key);
                //                TopicDescription topicDescription = value.get();
                //                topicDescription.partitions().forEach(item -> System.out.println(item));
            }
        }catch (InterruptedException | ExecutionException e){
            System.out.println("--------------- Kafka Exception : " + e);
        }
    }

    private static Set<String> listTopicNames(){
        Set<String> names = Sets.newHashSet();
        try (KafkaAdminClient client =  getKafkaAdminClient()){
            ListTopicsResult topics = client.listTopics(new ListTopicsOptions().timeoutMs(1000));
            names = topics.names().get();
        }catch (InterruptedException | ExecutionException e){
            System.out.println("--------------- Kafka Exception : " + e);
        }
        names.forEach(name -> System.out.println("Topic : "+name));
        return names;
    }

    private static KafkaAdminClient getKafkaAdminClient(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServer);
        properties.put("connections.max.idle.ms", 5000);
        properties.put("request.timeout.ms", 5000);
        return (KafkaAdminClient) KafkaAdminClient.create(properties);
    }

    private static boolean isKafkaServerValid(){
        try{
//            ClientUtils.parseAndValidateAddresses(Lists.newArrayList(kafkaServer));
            System.out.println("Kafka Server is valid");
            return true;
        }catch(Exception e){
            System.out.println("--------------- Kafka Exception : " + e);
        }
        System.out.println("Kafka Server is NOT valid");
        return false;
    }

}
