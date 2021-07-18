package com.alwin.eshop.storm;

import com.alwin.eshop.storm.bolt.LogParseBolt;
import com.alwin.eshop.storm.bolt.ProductCountBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class HotProductTopology {

    private static final String BOOTSTRAP_SERVERS = "192.168.31.11:9092,192.168.31.12:9092,192.168.31.13:9092";
    private static final String TOPIC = "access-log";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        ByTopicRecordTranslator<String,String> brt =
                new ByTopicRecordTranslator<>( (r) -> new Values(r.value(),r.topic()),new Fields("message", TOPIC));

        KafkaSpoutConfig<String,String> kafkaSpoutConfig = KafkaSpoutConfig.builder(BOOTSTRAP_SERVERS, TOPIC)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "eshop-storm")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                .setOffsetCommitPeriodMs(1000)
                .setRecordTranslator(brt)
                //.setRetry(getRetryService())
                .setMaxUncommittedOffsets(100)
                .build();

        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        builder.setSpout("KafkaSpout", kafkaSpout, 1);
        builder.setBolt("LogParseBolt", new LogParseBolt(), 2)
                .setNumTasks(4)
                .shuffleGrouping("KafkaSpout");
        builder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
                .setNumTasks(4)
                .fieldsGrouping("LogParseBolt", new Fields("productId"));

        Config config = new Config();

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = null;
            try {
                cluster = new LocalCluster();
                cluster.submitTopology("HotProductTopology", config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
            Utils.sleep(30000);
            assert cluster != null;
            cluster.shutdown();
        }
    }

/*    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(1000), TimeInterval.microSeconds(2),
                10, TimeInterval.seconds(10));
    }*/

}
