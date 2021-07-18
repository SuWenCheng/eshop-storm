package com.alwin.eshop.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class AccessKafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    

    @Override
    public void nextTuple() {
        ArrayBlockingQueue<String> queue = KafkaQueue.getQueue();
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                collector.emit(new Values(message));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("message"));
    }
}
