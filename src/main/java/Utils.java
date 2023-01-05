import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Utils {

    private static final Logger log = LogManager.getLogger(Utils.class);


     static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        PrometheusHttpClient.committedOffsets = PrometheusHttpClient.admin.listConsumerGroupOffsets(PrometheusHttpClient.CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();

        for (TopicPartitionInfo p : PrometheusHttpClient.td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(PrometheusHttpClient.topic, p.partition()), OffsetSpec.latest());
            requestTimestampOffsets2.put(new TopicPartition(PrometheusHttpClient.topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(PrometheusHttpClient.topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(PrometheusHttpClient.sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                PrometheusHttpClient.admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                PrometheusHttpClient.admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                PrometheusHttpClient.admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : PrometheusHttpClient.td.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }
        for (TopicPartitionInfo p : PrometheusHttpClient.td.partitions()) {
            TopicPartition t = new TopicPartition(PrometheusHttpClient.topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();
            long committedoffset = PrometheusHttpClient.committedOffsets.get(t).offset();
            PrometheusHttpClient.partitions.get(p.partition()).setLag(latestOffset - committedoffset);
            PrometheusHttpClient.partitions2.get(p.partition()).setLag(latestOffset - committedoffset);

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
                PrometheusHttpClient.partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                PrometheusHttpClient.partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);
                PrometheusHttpClient.partitions3.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);



            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / PrometheusHttpClient.doublesleep;
                //if(currentPartitionArrivalRate==0) continue;
                //TODO only update currentPartitionArrivalRate if (currentPartitionArrivalRate - previousPartitionArrivalRate) < 10 or threshold
                // currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                //TODO break
                PrometheusHttpClient.partitions.get(p.partition()).setArrivalRate(currentPartitionArrivalRate);
                PrometheusHttpClient.partitions2.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);
                PrometheusHttpClient.partitions3.get(p.partition()).setArrivalRate(currentPartitionArrivalRate*0.7);



            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;
        }
        //report total arrival only if not zero only the loop has not exited.
        log.info("totalArrivalRate for topic 1 {}", totalArrivalRate);



        if(Math.abs(PrometheusHttpClient.previoustotalArrivalRate - totalArrivalRate) > 15) {
            log.info("sorry");
            return;
        }

        PrometheusHttpClient.previoustotalArrivalRate = totalArrivalRate;

        PrometheusHttpClient.youMightWanttoScaleUsingBinPack();
        PrometheusHttpClient.youMightWanttoScaleUsingBinPack2();
        PrometheusHttpClient.youMightWanttoScaleUsingBinPack3();




     /*   if (Duration.between(lastScaletime, Instant.now()).getSeconds()> 15)
            youMightWanttoScaleUsingBinPack();*/
    }
}
