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
    static String topic3 = "testtopic5";
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    static int size=1;
    static int size2=1;
    static int size3=1;

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
    static TopicDescription td3;


    static DescribeTopicsResult tdr;
    static DescribeTopicsResult tdr2;
    static DescribeTopicsResult tdr3;
    static ArrayList<Partition> partitions = new ArrayList<>();
    static ArrayList<Partition> partitions2 = new ArrayList<>();
    static ArrayList<Partition> partitions3 = new ArrayList<>();
    static double doublesleep;
    static Instant lastScaletime1;
    static Instant lastScaletime2;
    static Instant lastScaletime3;
    //static long totalArrivalRate = 0;
    static long previoustotalArrivalRate = 20;







/*    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(consumerGroupList);
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
        size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        size2 = consumerGroupDescriptionMap.get("testgroup2").members().size();

        log.info("number of consumers of group 1 {}", size);
        log.info("number of consumers of group 2 {}", size2);
    }*/

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
        tdr3 = admin.describeTopics(Collections.singletonList("testtopic5"));
        td2 = tdr2.values().get(topic2).get();
        td3 = tdr3.values().get(topic3).get();
        log.info("Hello");

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

        for (TopicPartitionInfo p : td3.partitions()) {
            partitions3.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic1 has the following partitions {}", td.partitions().size());
        log.info("topic2 has the following partitions {}", td2.partitions().size());
        log.info("topic5 has the following partitions {}", td3.partitions().size());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        doublesleep = (double) sleep / 1000.0;
        try {
            //Initial delay so that the producer has started.
            lastScaletime1 = Instant.now();
            lastScaletime2 = Instant.now();
            lastScaletime3 = Instant.now();
            log.info("warming for 30 secs");
            Thread.sleep(30*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            log.info("New Iteration:");
            try {
               // queryConsumerGroup();
                Utils.getCommittedLatestOffsetsAndLag();
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



     static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler for cg1");
       // int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        if(Duration.between(lastScaletime1, Instant.now()).toSeconds() >= 15 ) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
        log.info("===================================");
    }


     static void youMightWanttoScaleUsingBinPack2() {
        log.info("Calling the bin pack scaler for cg2");
        if(Duration.between(lastScaletime2, Instant.now()).toSeconds() >= 15 ) {
            scaleAsPerBinPack2(size2);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
        log.info("===================================");
    }

     static void youMightWanttoScaleUsingBinPack3() {
        log.info("Calling the bin pack scaler for cg");

        if(Duration.between(lastScaletime3, Instant.now()).toSeconds() >= 15 ) {
            scaleAsPerBinPack3(size3);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
        log.info("===================================");

    }



     static void scaleAsPerBinPack3(int currentsize) {
        log.info("Currently we have this number of consumers group1 {}", currentsize);
        int neededsize = binPackAndScale3();
        log.info("We currently need the following consumers for group3 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale > 0 ) {

            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by group3 {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec5").scale(neededsize);
                //scaling CG2 as well
                log.info("I have Upscaled group3 you should have {}", neededsize);
                size3 = neededsize;
            }
            lastScaletime3 = Instant.now();
        }
        else {
            int neededsized = binPackAndScaled3();
            int replicasForscaled =  currentsize -neededsized;
            if(replicasForscaled>0) {
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec5").scale(neededsized);
                    log.info("I have downscaled group3 you should have {}", neededsized);
                    size3 = neededsized;
                }
                lastScaletime3 = Instant.now();
            }
        }
    }


    public static void scaleAsPerBinPack2(int currentsize) {
        log.info("Currently we have this number of consumers group2 {}", currentsize);
        int neededsize = binPackAndScale2();
        log.info("We currently need the following consumers for group2 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - currentsize;
        if (replicasForscale > 0 ) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by group2 {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);
                //scaling CG2 as well
                log.info("I have Upscaled group2 you should have {}", neededsize);
                size2 = neededsize;
            }
            lastScaletime2 = Instant.now();
        }
        else {
            int neededsized = binPackAndScaled2();
            int replicasForscaled =  currentsize -neededsized;
            if(replicasForscaled>0) {
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsized);
                    //scaling CG2 as well

                    log.info("I have downscaled group2 you should have {}", neededsized);
                    size2 = neededsized;

                }
                lastScaletime2 = Instant.now();
            }
        }
    }




    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers group1 {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - currentsize;
      if (replicasForscale > 0 ) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by group1 {}", replicasForscale);
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    log.info("I have Upscaled group1you should have {}", neededsize);
                    size= neededsize;
                }
            lastScaletime1 = Instant.now();
        }
      else {
          int neededsized = binPackAndScaled();
          int replicasForscaled =  currentsize -neededsized;
          if(replicasForscaled>0) {
              try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                  k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsized);
                  log.info("I have downscaled group1 you should have {}", neededsized);
                  size= neededsized;

              }
              lastScaletime1 = Instant.now();
          }
        }
    }


    private static int binPackAndScale3() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions3);
        dynamicAverageMaxConsumptionRate = 175.0;/*95.0*0.8*/;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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



    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 175.0;/*95.0 * 0.8*/;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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
        return consumers.size();
    }


    private static int binPackAndScaled3() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions3);
        dynamicAverageMaxConsumptionRate = 175.0*0.6;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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




    private static int binPackAndScale2() {
        log.info("Inside binPackAndScale2 ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions2);
        dynamicAverageMaxConsumptionRate = 175.0;/*95.0*0.8*/;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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

    private static int binPackAndScaled() {
        log.info("Inside binPackAndScaled ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 175.0*0.6;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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



    private static int binPackAndScaled2() {
        log.info("Inside binPackAndScaled ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions2);
        dynamicAverageMaxConsumptionRate = 175.0*0.6;//450.0;//230.0;//450.0;//95.0; //450.0; //240.0;

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





















}



