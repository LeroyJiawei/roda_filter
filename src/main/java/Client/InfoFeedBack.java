package Client;

import MySerdes.ValueSerde;
import Structure.EventStringMatchResult;
import Structure.EventVal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.InfluxdbUtil;

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
        String KafkaServer = properties.getProperty("SinkKafkaServer");
        String influx_filename = properties.getProperty("InfluxdbConfigFile");


        // define the variable of kafka connection prop
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaServer);
        props.put("group.id", "decoder-Consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", ValueSerde.EventStringMatchResultDeserde.class.getName());




        // create consumer
        KafkaConsumer<String, EventStringMatchResult> consumer = new KafkaConsumer<>(props);
        //creat producer

        InfluxdbUtil influx = InfluxdbUtil.setUp(influx_filename,"clientTest");


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
                ConsumerRecords<String, EventStringMatchResult> records = consumer.poll(70);

                long tmpArriveTime = System.currentTimeMillis();
                for (ConsumerRecord<String, EventStringMatchResult> record : records) {
                    EventVal eVal = record.value().eval;
                    eVal.EventGetTime = tmpArriveTime;

                    influx.consumerInsert(eVal);
                }
            }
        } catch (Throwable e) {
            System.out.println("error while consume");
            e.printStackTrace();
            consumer.close();
            System.exit(1);
        }
        System.exit(0);
    }
}