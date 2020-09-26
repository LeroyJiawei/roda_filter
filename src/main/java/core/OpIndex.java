package core;

import MySerdes.ValueSerde;
import Structure.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.*;
import org.apache.log4j.*;
import utils.MatchSendStruct;
import utils.InfluxdbUtil;
import org.apache.commons.lang.StringUtils;


public class OpIndex {

    private final static int STOCK_NUM = 2;
    private final static int OP_NUM = 2;
    private final static int MAX_SUB_NUM = 300000;
    private final static int MAX_THREAD_NUM = 32;

    private static int ATTRIBUTE_NUM = 60;
    private static int CurrentMatchThreadNum = 10;

    private static Index_Op[][] indexLists = new Index_Op[STOCK_NUM][ATTRIBUTE_NUM];
    private static final SubSet[][][] subSets = new SubSet[STOCK_NUM][ATTRIBUTE_NUM][MAX_SUB_NUM];
    private static MatchSendStruct MatchResultHashBuf;

    // adjust variables
    private static final double BASE_RATE = 0.3;
    private static int EventNum = 0;
    private static final int MatchWinSize = 20;
    private static final int MatchWinAvgSize = 20;
    private static final int ExpMatchTime = 30;
    private static boolean adjustSwitch = false;
    private static double LastMatchTime = 0;
    private static int LastMatchThread = 0;
    private static double AvgMatchTime = 0;


    private static final ConNum[] conNum = new ConNum[STOCK_NUM];
    private static final Bucket[][] ThreadBucket = new Bucket[STOCK_NUM][MAX_THREAD_NUM];

    private static InfluxdbUtil influx;

    // main
    public static void main(String[] args) {
        // parse command line arguments
        if(args.length < 2){
            System.err.println("Required more arguments\nUsage: java -j OpIndex -configFilePath -maxAttrNum");
            System.exit(1);
        }
        String configFileName = "";
        try{
            configFileName = args[0];
            ATTRIBUTE_NUM = Integer.parseInt(args[2]);
            indexLists = new Index_Op[STOCK_NUM][ATTRIBUTE_NUM];
        } catch (Throwable e){
            System.err.println("Usage: REIN_dynamic -matchconfigfile -maxattr");
            e.printStackTrace();
            System.exit(1);
        }

        // read config file
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

        // set logger
        Logger logger = Logger.getLogger(OpIndex.class);
        /** 当构建容器时，注释以下内容**/
        SimpleLayout layout = new SimpleLayout();
        try{
            FileAppender logFileAppender=new FileAppender(layout, properties.getProperty("LogFile"));
            logger.addAppender(logFileAppender);
        } catch (Throwable e) {
            System.err.println("Failed to pen log file!");
        }
        logger.setLevel(Level.INFO);

        // kafka cluster connection settings
        String SinkKafkaServer = properties.getProperty("SinkKafkaServer");
        String SourceKafkaServer = properties.getProperty("SourceKafkaServer");
        // stream settings
        Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final StreamsBuilder index_builder = new StreamsBuilder();
        final StreamsBuilder match_builder = new StreamsBuilder();

        KStream<String, SubscribeVal> subscribe = index_builder.stream("Sub",
                Consumed.with(Serdes.String(), new ValueSerde.SubscribeSerde()));
        KStream<String, EventVal> event = match_builder.stream("Event",
                Consumed.with(Serdes.String(), new ValueSerde.EventSerde()));

        // producer config settings
        Properties ProducerProps =  new Properties();
        ProducerProps.put("bootstrap.servers", SinkKafkaServer);
        ProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ProducerProps.put("value.serializer", ValueSerde.EventStringMatchResultSerde.class.getName());
        KafkaProducer<String, EventStringMatchResult> RodaSender = new KafkaProducer<>(ProducerProps);

        // influxdb config
        String influx_filename = properties.getProperty("InfluxdbConfigFile");
        influx = InfluxdbUtil.setUp(influx_filename, "matchTime","matcherState");

        //initialize indexLists
        for(int j = 0; j < STOCK_NUM; j++){
            conNum[j] = new ConNum(ATTRIBUTE_NUM);
            for(int i = 0; i < ATTRIBUTE_NUM; i++){
                indexLists[j][i] = new Index_Op(OP_NUM, ATTRIBUTE_NUM);
            }
            for(int i= 0; i < MAX_THREAD_NUM; i++){
                ThreadBucket[j][i] = new Bucket(ATTRIBUTE_NUM);
            }

            /** Attention, add ThreadBucket initialize **/
            // Empty ThreadBucket
            for(int i = 0;i < CurrentMatchThreadNum; i++){
                ThreadBucket[j][i].executeNum = 0;
                for(int k = 0;k<ATTRIBUTE_NUM;k++){
                    ThreadBucket[j][i].bitSet[k] = false;
                }
            }
            boolean[] bit = new boolean[ATTRIBUTE_NUM];
            for(int i=0;i<ATTRIBUTE_NUM;i++)
                bit[i]=false;
            for(int i=0;i<ATTRIBUTE_NUM;i++){
                int max = 0;
                int n = 0;//每次选中约束数目最大的属性id
                for(int k=0;k<ATTRIBUTE_NUM;k++){
                    if(bit[k])continue;
                    if(max < conNum[j].AttriConNum[k]){
                        max = conNum[j].AttriConNum[k];
                        n = k;
                    }
                }
                bit[n] = true;
                int t = 0;
                int min = Integer.MAX_VALUE;
                for(int th=0;th<CurrentMatchThreadNum;th++){
                    if(min > ThreadBucket[j][th].executeNum){
                        min = ThreadBucket[j][th].executeNum;
                        t = th;
                    }
                }
                ThreadBucket[j][t].executeNum += max;
                ThreadBucket[j][t].bitSet[n] = true;
            }
        }



        // create match thread pool and summary thread pool
        ThreadPoolExecutor executorMatch = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
        ThreadPoolExecutor executorSend = new ThreadPoolExecutor(8, 8,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        // parallel model of match
        class Parallel implements Runnable{
            private final int threadIdx;
            private final EventVal v;
            private final CountDownLatch latch;
            private Parallel(int threadIdx, EventVal val, CountDownLatch latch){
                this.threadIdx = threadIdx;
                this.v = val;
                this.latch = latch;
            }
            private void Match(){
                int stock_id = this.v.StockId;
                int attribute_num = v.AttributeNum; // get atrribute number
                for (int i = 0; i < attribute_num; i++) {

                    // check job assignment, if it is not assgined for current thread, jump
                    if(!ThreadBucket[stock_id][threadIdx].bitSet[i])
                        continue;

                    // get current attribute id
                    int attribute_id = this.v.eventVals[i].attributeId;
                    if(indexLists[stock_id][attribute_id].Pivot){
                        for(int r = 0; r < attribute_num; r++){
                            double val = this.v.eventVals[r].val;
                            if(indexLists[stock_id][attribute_id].opList.opBuckets[0].bits[r]) {
                                for (List e : indexLists[stock_id][attribute_id].opList.opBuckets[0].opBucketLists[r].opBucketList) {
                                    if (e.val <= val) {
                                        try {
                                            subSets[stock_id][attribute_id][e.Id].sem.acquire(2);
                                            subSets[stock_id][attribute_id][e.Id].un_matched_num--;
                                        } catch (InterruptedException w) {
                                            w.printStackTrace();
                                        } finally {
                                            subSets[stock_id][attribute_id][e.Id].sem.release(2);
                                        }
                                        if (subSets[stock_id][attribute_id][e.Id].un_matched_num == 0) {
                                            int y = e.Id;
                                            MatchResultHashBuf.subSet.add(subSets[stock_id][attribute_id][y].SubId);
                                        }
                                    }
                                }
                            }

                            if(indexLists[stock_id][attribute_id].opList.opBuckets[1].bits[r]) {
                                for (List e : indexLists[stock_id][attribute_id].opList.opBuckets[1].opBucketLists[r].opBucketList) {
                                    if (e.val >= val) {
                                        try {
                                            subSets[stock_id][attribute_id][e.Id].sem.acquire(2);
                                            subSets[stock_id][attribute_id][e.Id].un_matched_num--;
                                        } catch (InterruptedException w) {
                                            w.printStackTrace();
                                        } finally {
                                            subSets[stock_id][attribute_id][e.Id].sem.release(2);
                                        }
                                        if (subSets[stock_id][attribute_id][e.Id].un_matched_num == 0) {
                                            int y = e.Id;
                                            MatchResultHashBuf.subSet.add(subSets[stock_id][attribute_id][y].SubId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            public void run(){
                this.Match();
                this.latch.countDown();
            }
        }


        // handle subscription stream
        subscribe.foreach((k,v)->{
            final  String subId = v.SubId;
            final int stock_id = v.StockId;
            final int pivotId = v.Pivot_Attri_Id;
            final int sub_num_id = indexLists[stock_id][pivotId].SubNum++;
            final int attributeNum = v.AttributeNum;

            conNum[stock_id].ConSumNum++;
            conNum[stock_id].AttriConNum[pivotId]++;

//            logger.info(String.format("total:%d, Client Name:%s, Client Num Id:%d, " +
//                            "Sub Stock Id:%d, Attribute Num:%d, apply-time=%d",
//                    conNum[stock_id].ConSumNum,subId,sub_num_id,
//                    stock_id,attributeNum,System.currentTimeMillis()-v.generateTime));

            if(conNum[stock_id].ConSumNum % 10000 == 0){
                logger.info(String.format("total:%d, Client Name:%s, Client Num Id:%d, Sub Stock Id:%d, Attribute Num:%d",
                        conNum[stock_id].ConSumNum,subId,sub_num_id,stock_id,attributeNum));
            }
            //insert set
            subSets[stock_id][pivotId][sub_num_id] = new SubSet(attributeNum, subId);
            //insert sub
            indexLists[stock_id][pivotId].Pivot = true;
            for(int i = 0; i < attributeNum; i++) {
                indexLists[stock_id][pivotId].opList.opBuckets[0].bits[v.subVals.get(i).attributeId] = true;
                indexLists[stock_id][pivotId].opList.opBuckets[1].bits[v.subVals.get(i).attributeId] = true;
                indexLists[stock_id][pivotId].opList.opBuckets[0].opBucketLists[v.subVals.get(i).attributeId].
                        opBucketList.add(new List(sub_num_id, v.subVals.get(i).min_val));
                indexLists[stock_id][pivotId].opList.opBuckets[1].opBucketLists[v.subVals.get(i).attributeId].
                        opBucketList.add(new List(sub_num_id, v.subVals.get(i).max_val));
            }
        });


        // handle event stream
        //matcher
        event.foreach((k, v) -> {
            int stock_id = v.StockId;

            //compute event access delay
            long tmpTime = System.currentTimeMillis();
            v.EventArriveTime = tmpTime - v.EventProduceTime;

            final CountDownLatch matchLatch = new CountDownLatch(CurrentMatchThreadNum);
            MatchResultHashBuf = new MatchSendStruct(v);
            // launch match
            tmpTime = System.nanoTime();
            for (int i = 0; i < CurrentMatchThreadNum; i++) {
                Parallel s = new Parallel(i, v, matchLatch);
                executorMatch.execute(s);
            }
            try {
                matchLatch.await();
            }catch (Exception e){
                e.printStackTrace();
            }

            double per_match_time = (System.nanoTime() - tmpTime)/1000000.0;
            // batch the result to a total string
            int resultSize = MatchResultHashBuf.subSet.size();
            String eventStrResult = StringUtils.join(
                    MatchResultHashBuf.subSet.toArray(), ",");
            EventStringMatchResult eventStrMR = new EventStringMatchResult(v,eventStrResult);
            // send result
            try{
                ProducerRecord<String, EventStringMatchResult> record =
                        new ProducerRecord<>("MatchStringResult", eventStrMR);
                RodaSender.send(record);
            } catch (Exception e){
                logger.error("Send error");
                e.printStackTrace();
            }
            double per_detain_time = (System.nanoTime() - tmpTime)/1000000.0;


            // reset un_matched_num
            int stockId = v.StockId;
            for(int i = 0; i < ATTRIBUTE_NUM; i++){
                for(int j=0;j<indexLists[stockId][i].SubNum;j++){
                    subSets[stockId][i][j].un_matched_num = subSets[stockId][i][j].attribute_num << 1;
                }
            }
            EventNum++;
            logger.info(String.format("Event-%d: subscriptionNum=%d, matchTime=%f, matchThreadNum=%d, " +
                            " matchResultSize=%d, detain_time=%f",
                    EventNum,conNum[stock_id].ConSumNum, per_match_time,
                    CurrentMatchThreadNum,resultSize, per_detain_time));

            AvgMatchTime = (EventNum > MatchWinSize - MatchWinAvgSize) ?
                    AvgMatchTime + per_match_time : AvgMatchTime;

            // send state to influxDB
            influx.matcherInsert(v.EventProduceTime, per_match_time, per_detain_time);

            // dynamic adjust part
            if(EventNum == MatchWinSize) {
                long adjustTmpTime = System.nanoTime();

                AvgMatchTime = AvgMatchTime / MatchWinAvgSize;

                // adjust match thread number
                int thread_tmp = CurrentMatchThreadNum;

                if(!adjustSwitch && AvgMatchTime > ExpMatchTime * (1 - BASE_RATE)){
                    LastMatchThread = CurrentMatchThreadNum;
                    CurrentMatchThreadNum++;
                    LastMatchTime = AvgMatchTime;
                    adjustSwitch = true;
                }else if(AvgMatchTime > ExpMatchTime * (1 - BASE_RATE)) {
                    if(CurrentMatchThreadNum == 1){
                        LastMatchThread = CurrentMatchThreadNum;
                        CurrentMatchThreadNum = 2;
                        LastMatchTime = AvgMatchTime;
                    }else{
                        double e = (AvgMatchTime - LastMatchTime)/LastMatchTime * CurrentMatchThreadNum/LastMatchThread;
                        if(CurrentMatchThreadNum < LastMatchThread)
                            e = -e;
                        int o = (int)(CurrentMatchThreadNum * (1 - e));
                        o = Math.min(o,MAX_THREAD_NUM);
                        if (o > 0 && o != CurrentMatchThreadNum) {
                            LastMatchThread = CurrentMatchThreadNum;
                            LastMatchTime = AvgMatchTime;
                            CurrentMatchThreadNum = o;
                        }
                    }
                }else if(ExpMatchTime * (1 - BASE_RATE) > AvgMatchTime){
                    if(CurrentMatchThreadNum > 1){
                        LastMatchThread = CurrentMatchThreadNum;
                        LastMatchTime = AvgMatchTime;
                        CurrentMatchThreadNum--;
                    }
                }

                // reset related data structures
                if(CurrentMatchThreadNum != thread_tmp){
                    /* Task division */
                    // Empty ThreadBucket
                    for(int i = 0;i < CurrentMatchThreadNum; i++){
                        ThreadBucket[stock_id][i].executeNum = 0;
                        for(int j = 0;j<ATTRIBUTE_NUM;j++){
                            ThreadBucket[stock_id][i].bitSet[j] = false;
                        }
                    }
                    boolean[] bit = new boolean[ATTRIBUTE_NUM];
                    for(int i=0;i<ATTRIBUTE_NUM;i++)
                        bit[i]=false;
                    for(int i=0;i<ATTRIBUTE_NUM;i++){
                        int max = 0;
                        int n = 0;//每次选中约束数目最大的属性id
                        for(int j=0;j<ATTRIBUTE_NUM;j++){
                            if(bit[j])continue;
                            if(max < conNum[stock_id].AttriConNum[j]){
                                max = conNum[stock_id].AttriConNum[j];
                                n = j;
                            }
                        }
                        bit[n] = true;
                        int t = 0;
                        int min = Integer.MAX_VALUE;
                        for(int j=0;j<CurrentMatchThreadNum;j++){
                            if(min > ThreadBucket[stock_id][j].executeNum){
                                min = ThreadBucket[stock_id][j].executeNum;
                                t = j;
                            }
                        }
                        ThreadBucket[stock_id][t].executeNum += max;
                        ThreadBucket[stock_id][t].bitSet[n] = true;
                    }
                }

                // print the statistic information
                logger.info(
                        String.format(
                                "Expected_match_time: %d, Aver_match_Time: %f, Last_match_thread: %d, " +
                                        "New_thread_num: %d, Thread_adjust_time: %f\n\n",
                                ExpMatchTime, AvgMatchTime,
                                LastMatchThread, CurrentMatchThreadNum,
                                (System.nanoTime() - adjustTmpTime)/1000000.0
                        ));
                AvgMatchTime = 0;
                EventNum = 0;

                // matcher state statistics
                influx.mactcherStateInsert(MAX_THREAD_NUM, ExpMatchTime, BASE_RATE, LastMatchThread,
                        AvgMatchTime,CurrentMatchThreadNum);
            }
        });

        // construct subscribe stream
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_index1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SinkKafkaServer);
        final Topology index_topology = index_builder.build();
        final KafkaStreams stream_index = new KafkaStreams(index_topology, props);

        // construct match stream
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream_matcher1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SourceKafkaServer);
        final Topology matcher_topology = match_builder.build();
        final KafkaStreams stream_matcher = new KafkaStreams(matcher_topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                stream_index.close();
                stream_matcher.close();
                RodaSender.close();
                executorMatch.shutdown();
                executorSend.shutdown();
                latch.countDown();
            }
        });

        try {
            stream_index.start();
            stream_matcher.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
