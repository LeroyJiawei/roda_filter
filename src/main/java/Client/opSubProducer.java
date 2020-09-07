package Client;

import MySerdes.ValueSerde;
import Structure.SubscribeVal;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

public class opSubProducer {

    private static int MAX_ATTRIBUTE_NUM;
    private static final ArrayList<Pivot> Pivot_Attr = new ArrayList<>();

    static public int compare(Pivot o1, Pivot o2) {
        if ( o1.num > o2.num ) {
            return 1;
        } else {
            return -1;
        }
    }

    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("Usage: opSubProducer -configfile -subnum");
            System.exit(1);
        }
        String configFileName = "";
        int subNum = 0;
        try{
            configFileName = args[0];
            subNum = Integer.parseInt(args[1]);
        }catch (Throwable e){
            e.printStackTrace();
            System.exit(1);
        }
        
        Properties properties = new Properties();
        
        try {
            InputStream inputStream = new FileInputStream(new File(configFileName));
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            System.err.println("properties file open failed!");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("properties file read failed");
            e.printStackTrace();
        }
        String SubFile = properties.getProperty("SubFile");
        String KafkaServer = properties.getProperty("SinkKafkaServer");
        
        Properties Props =  new Properties();
        Props.put("bootstrap.servers", KafkaServer);
        Props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Props.put("value.serializer", ValueSerde.SubValSerde.class.getName());

        KafkaProducer<String, SubscribeVal> producer = new KafkaProducer<>(Props);

        Scanner s = null;
        try{
            File file = new File(SubFile);
            s = new Scanner(file);
            s.nextInt();
            MAX_ATTRIBUTE_NUM = s.nextInt();
            s.nextInt();
        }catch (Throwable e){
            e.printStackTrace();
            System.err.println("file read failed!");
            System.exit(1);
        }

        for(int i = 0; i < MAX_ATTRIBUTE_NUM; i++)
            Pivot_Attr.add(new Pivot(i));
        
	    while(true){
	        if(subNum==0)
	            break;
            for(int i = 0; i < subNum; i++) {
                String SubId = s.next();
                int StockId = s.nextInt();
                int AttributeNum = s.nextInt();
                int max = MAX_ATTRIBUTE_NUM;
                SubscribeVal sVal = new SubscribeVal(AttributeNum);
                sVal.SubId = SubId;
                sVal.StockId = StockId;
                for(int j = 0; j < sVal.AttributeNum; j++){
                    sVal.subVals.get(j).attributeId = s.nextInt();
                    sVal.subVals.get(j).min_val = s.nextDouble();
                    sVal.subVals.get(j).max_val = s.nextDouble();
                    for( int w = 0; w < MAX_ATTRIBUTE_NUM; w++ )
                        if( Pivot_Attr.get(w).Attr_id == sVal.subVals.get(j).attributeId ){
                            Pivot_Attr.get(w).num++;
                            break;
                        }
                }
                Pivot_Attr.sort(opSubProducer::compare);
                for(int j = 0; j < sVal.AttributeNum; j++){
                    int tmp = 0;
                    int m = 0;
                    while( sVal.subVals.get(j).attributeId != Pivot_Attr.get( m++ ).Attr_id ||
                            Pivot_Attr.get( m++ ).num == 0)
                        if( ++tmp >= MAX_ATTRIBUTE_NUM - 1 )break;
                    max = Math.min(max, tmp);
                }
                sVal.Pivot_Attri_Id = Pivot_Attr.get(max).Attr_id;
                //Record
                /** attention */
                sVal.generateTime = System.currentTimeMillis();
                ProducerRecord<String, SubscribeVal> record = new ProducerRecord<>("Sub", sVal);
                //send
                try {
                    producer.send(record).get();
                    System.out.println("Producer Send " + i + " Success!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
	    }
        producer.close();
        s.close();
    }
    static class Pivot{
        public int Attr_id;
        public int num;

        public Pivot(int i){
            this.Attr_id = i;
            this.num = 0;
        }
    }
}
