package com.alwin.eshop.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.alwin.eshop.storm.zk.ZookeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 商品访问次数统计bolt
 */
public class ProductCountBolt extends BaseRichBolt {

    private final static int TOP_N = 3;
    private static final String TASK_ID_NODE_DATA_PATH = "/taskid-list";
    private static final String HOT_PRODUCT_LIST_NODE_DATA_PATH_PREFIX = "/task-hot-product-list-";
    private final LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);
    private ZookeeperSession zkSession;
    private int taskId;
    private Logger logger = LoggerFactory.getLogger(ProductCountBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext context, OutputCollector collector) {
        zkSession = ZookeeperSession.getInstance();
        this.taskId = context.getThisTaskId();
        new Thread(new ProductCountThread()).start();
        initTaskId(taskId);
        logger.info("ProductCountBolt.prepare, ThreadName={}, taskId={}, productCountMap:{}",
                Thread.currentThread().getName(), taskId, productCountMap.hashCode());
    }

    private void initTaskId(int taskId) {
        zkSession.acquireDistributedLock();

        zkSession.createNode(TASK_ID_NODE_DATA_PATH);
        String taskIdList = zkSession.getNodeData(TASK_ID_NODE_DATA_PATH);
        logger.info("【ProductCountBolt获取到taskid list】taskidList=" + taskIdList);
        if (!"".equals(taskIdList)) {
            taskIdList += "," + taskId;
        } else {
            taskIdList += taskId;
        }
        zkSession.setNodeData(TASK_ID_NODE_DATA_PATH, taskIdList);
        logger.info("【ProductCountBolt设置taskid list】taskidList=" + taskIdList);
        zkSession.releaseDistributedLock();
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.getOrDefault(productId, 0L);
        count ++;
        productCountMap.put(productId, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private class ProductCountThread implements Runnable {

        @Override
        public void run() {
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<>();
            List<Long> productIdList = new ArrayList<>();

            while (true) {
                try {
                    topnProductList.clear();
                    productIdList.clear();

                    if (productCountMap.size() == 0) {
                        Utils.sleep(100);
                        continue;
                    }

                    logger.info("【ProductCountBolt打印productCountMap的长度】size=" + productCountMap.size());

                    for (Map.Entry< Long, Long > productCountEntry : productCountMap.entrySet()) {
                        if (topnProductList.size() == 0) {
                            topnProductList.add(productCountEntry);
                        } else {

                            boolean bigger = false;

                            for (int i = 0; i < topnProductList.size(); i++) {
                                Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);
                                if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                    int lastIndex = topnProductList.size() < TOP_N ? topnProductList.size() - 1 : TOP_N - 2;
                                    for (int j = lastIndex; j >= i; j--) {
                                        if (j + 1 == topnProductList.size()) {
                                            topnProductList.add(null);
                                        }
                                        topnProductList.set(j + 1, topnProductList.get(j));
                                    }
                                    topnProductList.set(i, productCountEntry);
                                    bigger = true;
                                    break;
                                }
                            }
                            if (!bigger && topnProductList.size() < TOP_N) {
                                topnProductList.add(productCountEntry);
                            }
                        }
                    }

                    for (Map.Entry<Long, Long> topnProductEntry : topnProductList) {
                        productIdList.add(topnProductEntry.getKey());
                    }

                    String topnProductListJson = JSONArray.toJSONString(productIdList);
                    String zkNodeDataPath = HOT_PRODUCT_LIST_NODE_DATA_PATH_PREFIX + taskId;
                    zkSession.createNode(zkNodeDataPath);
                    zkSession.setNodeData(zkNodeDataPath, topnProductListJson);
                    logger.info("【ProductCountBolt计算出一份top{}热门商品列表】zk path={}", TOP_N, zkNodeDataPath);

                    Utils.sleep(5000);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
