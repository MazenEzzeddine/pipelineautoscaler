import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;





public class PrometheusHttpClient {

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);
    static Long sleep;
    static String topic;
    static String topic2 = "testtopic2";
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    static int size;
    static int size2;
    static double dynamicAverageMaxConsumptionRate = 0.0;
    static double wsla = 5.0;
    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static Instant lastCGQuery;

    static List<String> consumerGroupList;


    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets2;


    //////////////////////////////////////////////////////////////////////////////
    static TopicDescription td;
    static TopicDescription td2;

    static DescribeTopicsResult tdr;
    static DescribeTopicsResult tdr2;
    static ArrayList<Partition> partitions = new ArrayList<>();
    static ArrayList<Partition> partitions2 = new ArrayList<>();
    static double doublesleep;
    static Instant lastScaletime1;
    static Instant lastScaletime2;




    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(consumerGroupList);
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
        size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        size2 = consumerGroupDescriptionMap.get("testgroup2").members().size();

        log.info("number of consumers of group 1 {}", size);
        log.info("number of consumers of group 2 {}", size2);
    }

    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        //cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr = admin.describeTopics(Collections.singletonList(topic));
        td = tdr.values().get(topic).get();
        tdr2 = admin.describeTopics(Collections.singletonList("testtopic2"));
        td2 = tdr2.values().get(topic2).get();
        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();
        lastCGQuery = Instant.now();

        consumerGroupList = new ArrayList<>();
        consumerGroupList.add(PrometheusHttpClient.CONSUMER_GROUP);
        consumerGroupList.add("testgroup2");

        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
            //partitions2.add(new Partition(p.partition(), 0, 0));
        }


        for (TopicPartitionInfo p : td2.partitions()) {
            partitions2.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic1 has the following partitions {}", td.partitions().size());
        log.info("topic2 has the following partitions {}", td.partitions().size());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        doublesleep = (double) sleep / 1000.0;
        try {
            //Initial delay so that the producer has started.
            lastScaletime1 = Instant.now();
            lastScaletime2 = Instant.now();

            Thread.sleep(30*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            log.info("New Iteration:");
            try {
                queryConsumerGroup();
                getCommittedLatestOffsetsAndLag();
               // getCommittedLatestOffsetsAndLag2();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Sleeping for {} seconds", sleep / 1000.0);
            log.info("End Iteration;");
            log.info("============================================");
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



    private static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler for topic1");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();

        if(Duration.between(lastScaletime1, Instant.now()).toSeconds() >= 20 ) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }


    private static void youMightWanttoScaleUsingBinPack2() {
        log.info("Calling the bin pack scaler for topic2");
        int size = consumerGroupDescriptionMap.get("testgroup2").members().size();

        if(Duration.between(lastScaletime2, Instant.now()).toSeconds() >= 20  ) {
            scaleAsPerBinPack2(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }



    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers group1 {}", currentsize);
        int neededsize = binPackAndScale();
        //int needsize2 = shallwescaletheother(neededsize);

        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by group1 {}", replicasForscale);

                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    //scaling CG2 as well

                    log.info("I have Upscaled group1you should have {}", neededsize);
                }
            lastScaletime1 = Instant.now();

        } else {


            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                //scaling CG2 as well

                log.info("I have downscaled group1 you should have {}", neededsize);
            }

            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaletime1 = Instant.now();
        }
    }



    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 95.0;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (/*cons.getRemainingLagCapacity() >=  partition.getLag()  &&*/
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer((String.valueOf(consumerCount)), (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }
        log.info(" The BP scaler recommended {}", consumers.size());
        // copy consumers and partitions for fair assignment
        return consumers.size();
    }



    public static void scaleAsPerBinPack2(int currentsize) {
        log.info("Currently we have this number of consumers gor group2{}", currentsize);
        int neededsize = binPackAndScale2();

        log.info("We currently need the following consumers for group2 (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale group2 by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);
                //scaling CG2 as well

                log.info("I have Upscaled group2 you should have {}", neededsize);
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaletime2 = Instant.now();

        } else {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);
                //scaling CG2 as well

                log.info("I have Upscaled group2 you should have {}", neededsize);
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaletime2 = Instant.now();

        }
    }



    private static int binPackAndScale2() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions2);
        dynamicAverageMaxConsumptionRate = 95.0;//230.0;//450.0;//240.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (/*cons.getRemainingLagCapacity() >=  partition.getLag()  &&*/
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer((String.valueOf(consumerCount)), (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }
        log.info(" The BP scaler recommended for second CG {}", consumers.size());
        // copy consumers and partitions for fair assignment


        return consumers.size();
    }










    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();

        for (TopicPartitionInfo p : td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
            requestTimestampOffsets2.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : td.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }
        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition t = new TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            partitions.get(p.partition()).setLag(latestOffset - committedoffset);
            partitions2.get(p.partition()).setLag(latestOffset - committedoffset);

            //TODO if abs(currentPartitionArrivalRate -  previousPartitionArrivalRate) > 15
            //TODO currentPartitionArrivalRate= previousPartitionArrivalRate;
            if(timeoffset2==timeoffset1)
                break;

            if (timeoffset2 == -1) {
                timeoffset2 = latestOffset;
            }
            if (timeoffset1 == -1) {
                // NOT very critical condition
                currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);


            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                //if(currentPartitionArrivalRate==0) continue;
                //TODO only update currentPartitionArrivalRate if (currentPartitionArrivalRate - previousPartitionArrivalRate) < 10 or threshold
                // currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                //TODO break
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);


            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;
        }
        //report total arrival only if not zero only the loop has not exited.
        log.info("totalArrivalRate for topic 1 {}", totalArrivalRate);


        youMightWanttoScaleUsingBinPack();
        youMightWanttoScaleUsingBinPack2();



     /*   if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 15)
            youMightWanttoScaleUsingBinPack();*/
    }


    private static void getCommittedLatestOffsetsAndLag2() throws ExecutionException, InterruptedException {
        committedOffsets2 = admin.listConsumerGroupOffsets("testgroup2")
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();

        for (TopicPartitionInfo p : td2.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic2, p.partition()), OffsetSpec.latest());
            requestTimestampOffsets2.put(new TopicPartition(topic2, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic2, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : td2.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }
        for (TopicPartitionInfo p : td2.partitions()) {
            TopicPartition t = new TopicPartition(topic2, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();
            long committedoffset = committedOffsets2.get(t).offset();
            partitions2.get(p.partition()).setLag(latestOffset - committedoffset);
            //TODO if abs(currentPartitionArrivalRate -  previousPartitionArrivalRate) > 15
            //TODO currentPartitionArrivalRate= previousPartitionArrivalRate;
            if(timeoffset2==timeoffset1)
                break;

            if (timeoffset2 == -1) {
                timeoffset2 = latestOffset;
            }
            if (timeoffset1 == -1) {
                // NOT very critical condition
                currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
             /*   log.info("Arrival rate into partition {} is {}", t.partition(), partitions.get(p.partition()).getArrivalRate());
                log.info("lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));*/
            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                //if(currentPartitionArrivalRate==0) continue;
                //TODO only update currentPartitionArrivalRate if (currentPartitionArrivalRate - previousPartitionArrivalRate) < 10 or threshold
                // currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                //TODO break
                partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                 /* log.info(" Arrival rate into partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getArrivalRate());

                log.info(" lag of  partition {} is {}", t.partition(),
                        partitions.get(p.partition()).getLag());
                log.info(partitions.get(p.partition()));*/
            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;
        }
        //report total arrival only if not zero only the loop has not exited.
        log.info("totalArrivalRate for topic 2 {}", totalArrivalRate);

        if(totalArrivalRate < 80) return;



       /* if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 15)
            youMightWanttoScaleUsingBinPack2();*/
    }

}



