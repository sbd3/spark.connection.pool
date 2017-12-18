package com.streaming.spark.demo.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.streaming.spark.demo.constants.Constants;
import com.streaming.spark.demo.constants.Constants.APP_PROPERTIES;
import com.streaming.spark.demo.constants.Constants.KAFKA_PROPERTIES;

/**
 * Main class for Spark Streaming execution
 */
public class SparkStreamingMain {

    /**
     * Initializes Spark Streaming context and process the Kafka input records
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        final JavaStreamingContext jssc = new JavaStreamingContext(getSparkConf(),
                Durations.milliseconds(Constants.STREAM_BATCH_DURATION_MILLIS));
        final JavaInputDStream<ConsumerRecord<byte[], byte[]>> rawStream = KafkaSource.getKafkaDirectStream(jssc);
        rawStream.foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {
                try (PartitionContextImpl context = PartitionContextImpl.getPartitionContext()) {
                    while (iter.hasNext()) {
                        ConsumerRecord<byte[], byte[]> r = iter.next();
                        if (r.value() == null) {
                            return;
                        }
                        // Do something with consumer record
                        byte[] result = process(r.value());
                        context.getKafkaProducer().send(
                                new ProducerRecord<byte[], byte[]>(Constants.KAFKA_OUTPUT_TOPIC, result),
                                new Callback() {
                                    @Override
                                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                                        // LOG.debug("Record sent to Kafka
                                        // Broker successfully!!!");
                                    }
                                });
                    }
                }
            });
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /**
     * Process record with business logic
     * @param value
     * @return Returns the processed value for input record
     */
    private static byte[] process(byte[] value) {
        return null;
    }

    /**
     * @return Returns Spark configuration object intialized
     */
    private static SparkConf getSparkConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName(Constants.STREAMING_APP_NAME);
        conf.set(APP_PROPERTIES.SPARK_SERIALIZER.getValue(), Constants.SPARK_SERIALIZER);
        conf.set(APP_PROPERTIES.SPARK_WAREHOUSE.getValue(), Constants.SPARK_WAREHOUSE);
        conf.setMaster(Constants.SPARK_MASTER);
        return conf;
    }

    private static class KafkaSource {

        /**
         * Creates Spark Streaming to Kafka Direct Stream for reading the data from Kafka input topics
         * @param jssc
         * @return
         */
        public static JavaInputDStream<ConsumerRecord<byte[], byte[]>> getKafkaDirectStream(JavaStreamingContext jssc) {
            JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream(jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<byte[], byte[]>Subscribe(getKafkaTopic(), getKafkaConf()));
            return stream;
        }

        /**
         * @return Returns Kafka Output Topic name
         */
        private static Set<String> getKafkaTopic() {
            Set<String> topics = new HashSet<>();
            topics.add(Constants.KAFKA_OUTPUT_TOPIC);
            return topics;
        }

        /**
         * @return Return Kafka configurations
         */
        private static Map<String, Object> getKafkaConf() {
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put(KAFKA_PROPERTIES.KAFKA_BOOTSTRAP_SERVERS.getValue(), Constants.KAFKA_BOOTSTRAP_SERVERS);
            kafkaParams.put(KAFKA_PROPERTIES.KAFKA_KEY_DESERIALIZER.getValue(), ByteArrayDeserializer.class);
            kafkaParams.put(KAFKA_PROPERTIES.KAFKA_VALUE_DESERIALIZER.getValue(), ByteArrayDeserializer.class);
            kafkaParams.put(KAFKA_PROPERTIES.KAFKA_GROUPID.getValue(), Constants.KAFKA_GROUP_ID);
            kafkaParams.put(KAFKA_PROPERTIES.KAFKA_ENABLE_AUTO_COMMIT.getValue(), Constants.KAFKA_ENABLE_AUTO_COMMIT);
            return kafkaParams;
        }
    }

}
