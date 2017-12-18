package com.streaming.spark.demo;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;

public abstract class KafkaProducerFactoryStub<K, V> extends BasePooledObjectFactory<KafkaProducer<K, V>> {

    /* 
     * Validate the pooled object
     * @see org.apache.commons.pool2.BasePooledObjectFactory#validateObject(org.apache.commons.pool2.PooledObject)
     */
    public abstract boolean validateObject(PooledObject<KafkaProducer<K, V>> pooledObject);
    
    /* Safely destroy the pool object by flushing the cached results (if any)
     * @see org.apache.commons.pool2.BasePooledObjectFactory#destroyObject(org.apache.commons.pool2.PooledObject)
     */
    public abstract void destroyObject(PooledObject<KafkaProducer<K, V>> pooledObject) throws Exception;
    
    /* 
     * Wrap KafkaProducer object with Connection Pool object
     * @see org.apache.commons.pool2.BasePooledObjectFactory#wrap(java.lang.Object)
     */
    public abstract PooledObject<KafkaProducer<K, V>> wrap(KafkaProducer<K, V> producer);
    
    /* 
     * Create Kafka Producer Object
     * @see org.apache.commons.pool2.BasePooledObjectFactory#create()
     */
    public abstract KafkaProducer<K, V> create() throws Exception;

}