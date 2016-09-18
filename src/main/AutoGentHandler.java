package main;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import util.Conf;
import util.CollectUtil;
 
public class AutoGentHandler {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    private static final Logger LOG = LogManager.getLogger(AutoGentHandler.class);
    public AutoGentHandler(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
        	LOG.error("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void run(int a_numThreads,Conf cf) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new CollectUtil(stream, threadNumber,cf));
            threadNumber++;
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
    public static void main(String[] args) {
    	Conf cf = new Conf();
		cf.setConfFile(args[0]);
		
		
        String zooKeeper = cf.getSingleString("broker");
       
        String groupId = cf.getSingleString("groupid");
        String topic = cf.getSingleString("topic");
        int threads = cf.getSinglefValue("thread");
 
        AutoGentHandler example = new AutoGentHandler(zooKeeper, groupId, topic);
        example.run(threads,cf);
 
        try {
            Thread.sleep(cf.getSinglefValue("sleep"));
        } catch (InterruptedException ie) {
 
        }
        example.shutdown();
    }
}