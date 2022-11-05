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
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;





public class PrometheusHttpClient {

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);

    static Instant lastUpScaleDecision;
    static Instant lastUpScaleDecision2;

    static Instant lastDownScaleDecision;
    static Long sleep;
    static String topic;
    static Long poll;
    static Long poll2;

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
    static Instant startTime;
    static Integer cooldown;

    static ArrayList<Partition> topicpartitions = new ArrayList<>();
    static ArrayList<Partition> topicpartitions2 = new ArrayList<>();

    static List<String> consumerGroupList;


    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static Instant lastScaleTime;
    static long joiningTime;

    //////////////////////////////////////////////////////////////////////////////
    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();

    static double doublesleep;
    static Instant lastScaletime;



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
      /*  log.info("inside read env");
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        poll = Long.valueOf(System.getenv("POLL"));
        poll2 = Long.valueOf(System.getenv("POLL2"));

        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        consumerGroupList = new ArrayList<>();
        consumerGroupList.add(PrometheusHttpClient.CONSUMER_GROUP);
        consumerGroupList.add("testgroup2");
        lastUpScaleDecision = Instant.now();
        lastScaleUpDecision = Instant.now();
        lastUpScaleDecision2 = Instant.now();*/




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
        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();
        lastCGQuery = Instant.now();

        consumerGroupList = new ArrayList<>();
        consumerGroupList.add(PrometheusHttpClient.CONSUMER_GROUP);
        consumerGroupList.add("testgroup2");

        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size());

    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////////////////////////////////////


    private static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        ///String partition = jobjpartition.getString("partition");
        /*log.info("the partition is {}", p);
        log.info("partition arrival rate: {}", Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble(jreq.getString(1));
    }


    private static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject) jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
       /* log.info("the partition is {}", p);
        log.info("partition lag  {}",  Double.parseDouble( jreq.getString(1)));*/
        return Double.parseDouble(jreq.getString(1));
    }


  /*  public static void main(String[] args) throws InterruptedException, ExecutionException {


        readEnvAndCrateAdminClient();

        log.info("Sleeping for 10 seconds");

        Thread.sleep(10000);
        log.info("Starting");

        HttpClient client = HttpClient.newHttpClient();
        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        String p0 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B1m%5D))";
        String p1 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B1m%5D))";
        String p2 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B1m%5D))";
        String p3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B1m%5D))";
        String p4 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B1m%5D))";


        //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
        //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

        String all4 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
        String p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D";
        String p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D";
        String p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D";
        String p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D";
        String p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D";


        List<URI> partitions = new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(p0),
                    new URI(p1),
                    new URI(p2),
                    new URI(p3),
                    new URI(p4)

            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag = new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(p0lag),
                    new URI(p1lag),
                    new URI(p2lag),
                    new URI(p3lag),
                    new URI(p4lag)

            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        for (int i = 0; i <= 4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
            topicpartitions2.add(new Partition(i, 0, 0));

        }
        // log.info("created the 5 partitions");

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //topic2


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        while (true) {
            Instant start = Instant.now();

            List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());


            List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());


            int partitionn = 0;
            double totalarrivals = 0.0;
            for (CompletableFuture cf : partitionsfutures) {
                try {
                    topicpartitions.get(partitionn).setArrivalRate(parseJsonArrivalRate((String) cf.get(), partitionn), false);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                try {
                    totalarrivals += parseJsonArrivalRate((String) cf.get(), partitionn);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                partitionn++;
            }
            log.info("totalArrivalRate for topic 1 {}", totalarrivals);


            partitionn = 0;
            double totallag = 0.0;
            for (CompletableFuture cf : partitionslagfuture) {
                try {
                    topicpartitions.get(partitionn).setLag(parseJsonArrivalLag((String) cf.get(), partitionn).longValue(), false);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                try {
                    totallag += parseJsonArrivalLag((String) cf.get(), partitionn);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                partitionn++;
            }


            log.info("totalLag for topic1 {}", totallag);
            Instant end = Instant.now();
            log.info("Duration in seconds to query prometheus for " +
                            "arrival rate and lag and parse result of topic 1 {}",
                    Duration.between(start, end).toMillis());





          *//*  for (int i = 0; i<=4; i++) {
                log.info("partition {} has the following arrival rate {} and lag {}",  i, topicpartitions.get(i).getArrivalRate(),
                        topicpartitions.get(i).getLag()) ;
            }*//*


            //log.info("calling the scaler");


            try {
                queryConsumerGroup();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }


            //doForTopic2();
           // youMightWanttoScale(totalarrivals);

            youMightWanttoScaleUsingBinPack();

            log.info("sleeping for 5 s");
            log.info("==================================================");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }*/


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
            lastScaletime = Instant.now();
            Thread.sleep(30*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            log.info("New Iteration:");
            try {
                queryConsumerGroup();
                getCommittedLatestOffsetsAndLag();
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
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
       /* if(size==0)
            return;
        if(Duration.between(startTime, Instant.now()).toSeconds() <= 140 ) {

            log.info("Warm up period period has not elapsed yet not taking decisions");
            return;
        }*/
        if(Duration.between(lastScaletime, Instant.now()).toSeconds() >= 15 /*30*/ ) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }


    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

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
            log.info("We have to upscale by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                //scaling CG2 as well
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);

                log.info("I have Upscaled you should have {}", neededsize);
            }

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                        //scaling CG2 as well

                        log.info("I have Upscaled you should have {}", neededsize);
                    }
                }
            }).start();

          /*  new Thread(new Runnable() {
                @Override
                public void run() {
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        //scaling CG2 as well
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);

                        log.info("I have Upscaled you should have {}", neededsize);
                    }
                }
            }).start();*/

            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
            lastScaletime = Instant.now();

        } else {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                //scaling down CG2 as well
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(neededsize);
                log.info("I have Downscaled you should have {}", neededsize);
                lastScaleUpDecision = Instant.now();
                lastScaleDownDecision = Instant.now();
                lastCGQuery = Instant.now();
            }
            lastScaletime = Instant.now();

        }
    }



    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(partitions);
        dynamicAverageMaxConsumptionRate = 95.0;

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
                if (cons.getRemainingLagCapacity() >=  partition.getLag()  &&
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




    /*static void doForTopic2() throws ExecutionException, InterruptedException {

        HttpClient client2 = HttpClient.newHttpClient();
        String all32 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        String p02 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D%5B1m%5D))";
        String p12 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D%5B1m%5D))";
        String p22 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D%5B1m%5D))";
        String p32 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D%5B1m%5D))";
        String p42 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D%5B1m%5D))";


        //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
        //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

        String all42 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
        String p0lag2 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D";
        String p1lag2 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D";
        String p2lag2 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D";
        String p3lag2 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D";
        String p4lag2 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D";


        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(p02),
                    new URI(p12),
                    new URI(p22),
                    new URI(p32),
                    new URI(p42)

            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(p0lag2),
                    new URI(p1lag2),
                    new URI(p2lag2),
                    new URI(p3lag2),
                    new URI(p4lag2)

            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        Instant start = Instant.now();

        List<CompletableFuture<String>> partitionsfutures = partitions2.stream()
                .map(target -> client2
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture = partitionslag2.stream()
                .map(target -> client2
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partitionn = 0;
        double totalarrivals = 0.0;
        for (CompletableFuture cf : partitionsfutures) {
            try {
                topicpartitions2.get(partitionn).setArrivalRate(parseJsonArrivalRate((String) cf.get(), partitionn), false);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            try {
                totalarrivals += parseJsonArrivalRate((String) cf.get(), partitionn);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            partitionn++;
        }
        log.info("totalArrivalRate for topic 2 {}", totalarrivals);


        partitionn = 0;
        double totallag = 0.0;
        for (CompletableFuture cf : partitionslagfuture) {
            try {
                topicpartitions2.get(partitionn).setLag(parseJsonArrivalLag((String) cf.get(), partitionn).longValue(), false);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            try {
                totallag += parseJsonArrivalLag((String) cf.get(), partitionn);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            partitionn++;
        }


        log.info("totalLag for topic2 {}", totallag);
        Instant end = Instant.now();
        log.info("Duration in seconds to query prometheus for " +
                        "arrival rate and lag and parse result of topic 2 {}",
                Duration.between(start, end).toMillis());

        //youMightWanttoScale2(totalarrivals);

    }
*/

    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30) {
            log.info("Upscale logic, Up scale cool down has ended");

            scale(totalArrivalRate, size);
        }
    }


    private static void scale(double totalArrivalRate, int size) {

        int reco = (int) Math.ceil(totalArrivalRate / poll);
        log.info("recommended number of replicas for topic 1 {}", reco);

        if (reco != size && reco>=1) {
            log.info("scaling both CGs");
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(reco);
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(reco);

            }

            lastUpScaleDecision = Instant.now();
            lastUpScaleDecision2 = Instant.now();
            lastDownScaleDecision = Instant.now();

        }
        log.info("S(int) Math.ceil(totalArrivalRate / poll) {}  ", reco);
    }


    private static void youMightWanttoScale2(double totalArrivalRate) throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get("testgroup2").members().size();
        log.info("curent group 2 size is {}", size);

        if (Duration.between(lastUpScaleDecision2, Instant.now()).toSeconds() >= 30) {
            log.info("Upscale logic, Up scale cool down has ended");

            scale2(totalArrivalRate, size);
        }
    }


    private static void scale2(double totalArrivalRate, int size) {

        int reco = (int) Math.ceil(totalArrivalRate / poll2);
        log.info("recommended number of replicas {}", reco);

        if (reco != size) {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec2").scale(reco);

            }

            lastUpScaleDecision2 = Instant.now();
            lastDownScaleDecision = Instant.now();

        }

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
                partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
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
        log.info("totalArrivalRate {}", totalArrivalRate);


        // attention not to have this CG querying interval less than cooldown interval
      /*  if (Duration.between(lastCGQuery, Instant.now()).toSeconds() >= 30) {
            queryConsumerGroup();
            lastCGQuery = Instant.now();
        }*/
        //youMightWanttoScaleUsingBinPack();

    /*    if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 30)
        youMightWanttoScaleTrial2();*/

      /*  if(Math.abs(totalArrivalRate-previousTotalArrivalRate) > 10) {
            return;
        }

        previousTotalArrivalRate = totalArrivalRate;
*/


        if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 30)
            youMightWanttoScaleUsingBinPack();
    }

}



