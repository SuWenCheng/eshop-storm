package com.alwin.eshop.storm.spout;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class KafkaQueue {

    private static final String TOPIC = "access-log";

    private static ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);

    public void listen(List<ConsumerRecord<String, String>> records) {
        // log.info("receive: {}", records.size());
        records.forEach(record -> {
            String message = record.value();
            // log.info("=================收到的kafka消息：" + message);
            try {
                queue.put(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public static ArrayBlockingQueue<String> getQueue() {
        return queue;
    }

}
