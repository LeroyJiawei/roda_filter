package Client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Collections;
import java.util.Properties;

public class InfoFeedBack {
    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Usage: Consumer -matchconfigfile");
            System.exit(1);
        }

        String config_filename = "";
        try{
            config_filename = args[0];
        } catch (Throwable e){
            System.out.println("Usage: Consumer -matchconfigfile");
            e.printStackTrace();
            System.exit(1);
        }

        // read consumer config file
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(new File(config_filename));
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            System.err.println("properties file open failed!");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("properties file read failed");
            e.printStackTrace();
        }
        String KafkaServer = properties.getProperty("KafkaServer");
        String securityProto = properties.getProperty("security.protocol");
        String saslName = properties.getProperty("sasl.kerberos.service.name");

        // define the variable of kafka connection prop
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaServer);
        props.put("group.id", "docker-Consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", ValueSerde.EventValDeserde.class.getName());
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if(securityProto != null && saslName != null){
            props.put("security.protocol", securityProto);
            props.put("sasl.kerberos.service.name", saslName);
        } else {
            System.out.println("WARNING: no authentication configuration");
        }

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //creat producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //TopicPartition topicPartition = new TopicPartition("", 0);

        try (consumer) {
            consumer.subscribe(Collections.singletonList("MatchStringResult"));

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("consumer-shutdown-hook") {
                @Override
                public void run() {
                    consumer.wakeup();
                }
            });
            
            // loop to poll events
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(70);

                for (ConsumerRecord<String, String> record : records) {
                    // decode to get EventVal object
                    System.out.println(record.value());
                    ProducerRecord<String,String> proRecord =
                            new ProducerRecord<>("MatchStringResult-Another", record.value());
                    producer.send(proRecord);
                }
            }
        } catch (Throwable e) {
            producer.close();
            consumer.close();
            System.exit(1);
        }
        System.exit(0);
    }
}