package com.streaming.spark.demo.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streaming.spark.demo.KafkaProducerFactoryStub;
import com.streaming.spark.demo.constants.Constants;
import com.streaming.spark.demo.constants.Constants.KAFKA_PROPERTIES;

public class KafkaProducerFactoryImpl<K, V> extends KafkaProducerFactoryStub<K, V> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerFactoryImpl.class);
    
    private static GenericObjectPool<KafkaProducer<byte[], byte[]>> pool;
    
    private KafkaProducerFactoryImpl(){}
    
    public static GenericObjectPool<KafkaProducer<byte[], byte[]>> getObjectPool() {
        if(pool == null) {
            synchronized(GenericObjectPool.class) {
                if(pool == null) {
                    pool = new GenericObjectPool<>(new KafkaProducerFactoryImpl<>(), getPoolConfig());
                }
            }
        }
        return pool;
    }

    @Override
    public KafkaProducer<K, V> create() throws Exception {
        LOGGER.debug("Creating Kafka Producer client connection!!!");
        KafkaProducer<K, V> producer = new KafkaProducer<>(getKafkaConf());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }));
        return producer;
    }

    @Override
    public PooledObject<KafkaProducer<K, V>> wrap(KafkaProducer<K, V> producer) {
        return new DefaultPooledObject<>(producer);
    }
    
    @Override
    public void destroyObject(PooledObject<KafkaProducer<K, V>> pooledObject) throws Exception {
        pooledObject.getObject().flush();
        pooledObject.getObject().close();
        LOGGER.debug("Closed Kafka Producer client!!!");
    }
    
    @Override
    public boolean validateObject(PooledObject<KafkaProducer<K, V>> pooledObject) {
        return pooledObject.getObject().partitionsFor(Constants.KAFKA_OUTPUT_TOPIC).size() > 0;
    }
    
    private Map<String, Object> getKafkaConf() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(KAFKA_PROPERTIES.KAFKA_BOOTSTRAP_SERVERS.getValue(), Constants.KAFKA_BOOTSTRAP_SERVERS);
        kafkaParams.put(KAFKA_PROPERTIES.KAFKA_KEY_SERIALIZER.getValue(), ByteArraySerializer.class);
        kafkaParams.put(KAFKA_PROPERTIES.KAFKA_VALUE_SERIALIZER.getValue(), ByteArraySerializer.class);
        return kafkaParams;
    }
    
    private static GenericObjectPoolConfig getPoolConfig() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();  
        config.setMaxTotal(10);
        config.setMaxIdle(10);  
        config.setMinIdle(2);  
        config.setMaxWaitMillis(-1);  
        config.setBlockWhenExhausted(true);
        config.setTestOnBorrow(false);  
        config.setTestOnReturn(false);  
        config.setTestWhileIdle(true);  
        config.setMinEvictableIdleTimeMillis(10 * 60000L); // 10 mins  
        config.setTestWhileIdle(true);
        return config;
    }

}
