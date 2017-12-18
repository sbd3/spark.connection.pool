package com.streaming.spark.demo;

import java.io.Closeable;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Interface to create reusable Kafka Producer object across Spark Partitions
 */
public interface PartitionContextStub extends Closeable {

    /**
     * @return Returns Kafka Producer object
     */
    KafkaProducer<byte[], byte[]> getKafkaProducer();

}