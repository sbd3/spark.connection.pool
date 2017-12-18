package com.streaming.spark.demo.impl;

import java.io.Closeable;
import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;

public class PartitionContextImpl implements Closeable {
    
    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    
    public PartitionContextImpl(PartitionContextBuilder partitionContextBuilder) {
        this.kafkaProducer = partitionContextBuilder.kafkaProducer;
    }
    
    public static PartitionContextImpl getPartitionContext() throws Exception {
        return new PartitionContextBuilder().setKafkaProducer(KafkaProducerFactoryImpl.getObjectPool().borrowObject()).build();
    }
    
    public KafkaProducer<byte[], byte[]> getKafkaProducer() {
        return kafkaProducer;
    }
    
    @Override
    public void close() throws IOException {
        try {
            KafkaProducerFactoryImpl.getObjectPool().returnObject(this.kafkaProducer);
        } catch(Exception e) {
            if(this.kafkaProducer != null) {
                this.kafkaProducer.flush();
                this.kafkaProducer.close();
            }
        }
    }

    public static class PartitionContextBuilder {

        private KafkaProducer<byte[], byte[]> kafkaProducer;
        
        public PartitionContextBuilder setKafkaProducer(KafkaProducer<byte[], byte[]> kafkaProducer) {
            this.kafkaProducer = kafkaProducer;
            return this;
        }
        
        public PartitionContextImpl build() {
            return new PartitionContextImpl(this);
        }
        
    }

}
